// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lib/pq"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/shared/mlog"
	"github.com/mattermost/mattermost/server/public/shared/request"
	"github.com/mattermost/mattermost/server/v8/channels/app/platform"
	"github.com/mattermost/mattermost/server/v8/channels/app/platform/clustercommunity/bus"
)

const (
	// defaultMaxConns is the fallback pool size when Options.MaxConns is zero.
	// Each pod consumes defaultMaxConns + 2 PostgreSQL connections (listener +
	// leader-lock). At 5 pods the cluster uses (10+2)×5 = 60 connections,
	// well within PostgreSQL's default max_connections=100.
	// Override per-deployment with MM_CLUSTER_PG_MAX_CONNS.
	defaultMaxConns = 10

	// defaultWebConnRPCTimeout is the fallback for Options.WebConnRPCTimeout.
	defaultWebConnRPCTimeout = time.Second

	connLifetime = 10 * time.Minute
)

// platformDeps is the narrow interface postgres.Cluster needs from the
// upstream PlatformService. *platform.PlatformService satisfies it in
// production; tests provide a fake to avoid standing up a full DB-backed
// server. Heartbeat / node discovery extras are passed through Options
// as function closures rather than widening this interface, because the
// *platform.ClusterDiscoveryService type is not construct-able outside
// its own package — making it an interface dependency would force every
// test to hand-implement it.
type platformDeps interface {
	Log() mlog.LoggerIFace
	InvokeClusterLeaderChangedListeners()
	// ReloadConfig is called by the ClusterEventCommunityConfigReload handler
	// when a peer signals that the shared config store was updated.
	ReloadConfig() error
	// TotalWebsocketConnections feeds GetClusterStats.
	TotalWebsocketConnections() int
	// WebConnCountForUser returns the number of active websocket connections
	// on THIS node for the given user.
	WebConnCountForUser(userID string) int
	// GetPluginStatuses returns the plugin statuses registered on THIS node.
	GetPluginStatuses() (model.PluginStatuses, *model.AppError)
}

// Cluster is the PostgreSQL-backed implementation of einterfaces.ClusterInterface.
//
// Goroutines started by StartInterNodeCommunication:
//   - listenerLoop: drains pq.Listener.Notify and dispatches events.
//   - leaderLoop:   advisory-lock contention + fast-path PingContext.
//   - gcLoop:       periodic DELETE of expired cluster_messages rows.
//
// The heartbeat (cluster_discovery row + 60s ping) runs inside
// ClusterDiscoveryService, which manages its own goroutine.
type Cluster struct {
	*bus.HandlerRegistry

	ps     platformDeps
	opts   *Options
	node   *bus.NodeIdentity
	logger mlog.LoggerIFace

	dsn      string
	db       *sql.DB // short-lived command pool
	listener *pq.Listener

	// discoveryFactory / listNodesFunc inject heartbeat and peer-discovery
	// capabilities. They are nil in tests — heartbeat and GetClusterInfos
	// degrade to no-ops rather than panicking.
	discoveryFactory func() *platform.ClusterDiscoveryService
	listNodesFunc    func(clusterType, clusterName string) ([]*model.ClusterDiscovery, error)

	discovery *platform.ClusterDiscoveryService

	cancelCtx context.Context
	cancelFn  context.CancelFunc
	wg        sync.WaitGroup

	// Leader state: leaderConn pins a dedicated session that holds
	// pg_advisory_lock(leaderLockKey) for as long as we are leader. Closing
	// that Conn (explicitly, or by process exit) releases the lock.
	leaderMu    sync.Mutex
	leaderConn  *sql.Conn
	leaderCache atomic.Bool

	started atomic.Bool
	health  *bus.HealthTracker

	// rpc handles request/response flows for P2 methods (GetClusterStats).
	// Initialized in StartInterNodeCommunication so tests that skip Start
	// still see a usable Cluster.
	rpc *bus.RPC

	// Asynchronous send pipeline. SendClusterMessage enqueues onto sendCh
	// without blocking; sendWorkers drain it into PostgreSQL. This decouples
	// the WebSocket broadcast hot-path from PG round-trip latency under
	// 10w-user-scale load. When sendCh is full, messages are dropped and
	// counted in sendDropped — back-pressure beats blocking the caller.
	//
	// sendMu guards the *assignment* of sendCh during Start, so concurrent
	// enqueueSend calls see a fully-published channel (or nil pre-Start).
	// sendCh itself is never closed: workers drain and exit when cancelCtx
	// is done. Closing the channel would create a send-on-closed race with
	// concurrent enqueueSend, since `select case ch <- v` panics on a closed
	// channel even with a default branch.
	sendMu      sync.RWMutex
	sendCh      chan sendTask
	sendWg      sync.WaitGroup
	sendDropped atomic.Int64
}

// sendTask is the unit of work for the async send pipeline. payload is the
// already-encoded envelope (encoding happens on the caller goroutine so the
// worker only does I/O).
type sendTask struct {
	target  string
	payload []byte
}

// New constructs a postgres.Cluster. Returns an error if the DSN cannot be
// resolved or the initial sql.Open fails. Any such failure is logged by the
// factory and converted into a nil ClusterInterface so the server falls back
// to single-node mode rather than crashing.
// NewFromPlatform is the production constructor: it resolves the DSN from the
// Mattermost config if Options.DSN is empty, and wires up heartbeat/discovery
// against the real PlatformService. Tests use New(...) with a fake platformDeps
// and leave heartbeat disabled.
func NewFromPlatform(ps *platform.PlatformService, opts *Options) (*Cluster, error) {
	if opts.DSN == "" {
		if ps.Config() == nil || ps.Config().SqlSettings.DataSource == nil {
			return nil, errors.New("MM_CLUSTER_PG_DSN is empty and no Mattermost SqlSettings.DataSource configured")
		}
		opts.DSN = *ps.Config().SqlSettings.DataSource
	}

	c, err := New(ps, opts)
	if err != nil {
		return nil, err
	}

	// Wire discovery only after base construction succeeds; listNodesFunc is
	// bound to the live store and must be invokable for the entire lifetime
	// of the cluster.
	c.discoveryFactory = ps.NewClusterDiscoveryService
	c.listNodesFunc = func(clusterType, clusterName string) ([]*model.ClusterDiscovery, error) {
		return ps.Store.ClusterDiscovery().GetAll(clusterType, clusterName)
	}
	return c, nil
}

// New is the test-friendly constructor. It takes only the narrow platformDeps
// interface and leaves heartbeat / discovery disabled (they are injected
// separately in NewFromPlatform). An explicit DSN is required.
func New(ps platformDeps, opts *Options) (*Cluster, error) {
	if opts.DSN == "" {
		return nil, errors.New("postgres.New: Options.DSN is required; use NewFromPlatform for automatic fallback to Mattermost config")
	}
	if opts.ChannelName == "" {
		opts.ChannelName = "mm_cluster"
	}
	dsn := opts.DSN

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres cluster db: %w", err)
	}
	maxOpen := opts.MaxConns
	if maxOpen <= 0 {
		maxOpen = defaultMaxConns
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(max(2, maxOpen/4)) // idle ≤ 25% of open, at least 2
	db.SetConnMaxLifetime(connLifetime)

	// Validate the DSN early. An invalid DSN that only fails on first real
	// use would leave the server running with broken clustering and no
	// clear signal to the operator.
	pingCtx, cancelPing := context.WithTimeout(context.Background(), 5*time.Second)
	if err := db.PingContext(pingCtx); err != nil {
		cancelPing()
		db.Close()
		return nil, fmt.Errorf("ping postgres cluster db: %w", err)
	}
	cancelPing()

	node := bus.ResolveNodeIdentity(opts.NodeID, opts.ClusterName)

	c := &Cluster{
		HandlerRegistry: bus.NewHandlerRegistry(),
		ps:              ps,
		opts:            opts,
		node:            node,
		logger:          ps.Log(),
		dsn:             dsn,
		db:              db,
		health:          &bus.HealthTracker{},
	}

	// Apply schema immediately so subsequent inserts have a table to write to.
	// Fail loudly if the DB user lacks DDL privileges — clustering cannot work.
	schemaCtx, cancelSchema := context.WithTimeout(context.Background(), 5*time.Second)
	if err := c.ensureSchema(schemaCtx); err != nil {
		cancelSchema()
		db.Close()
		return nil, err
	}
	cancelSchema()

	// Wire the P2 RPC infrastructure and register handlers *here*, not in
	// StartInterNodeCommunication. Reason: the upstream Server calls
	// Channels().Start() before StartInterNodeCommunication, and plugin
	// initialization during that window can trigger GetPluginStatuses /
	// notifyPluginStatusesChanged, which dispatch into c.rpc.Broadcast.
	// If c.rpc were nil at that point, we'd crash with a nil-pointer
	// dereference. Handlers being registered before the listener loop runs
	// is harmless — dispatch is driven by that loop, so nothing fires
	// until StartInterNodeCommunication anyway.
	c.rpc = bus.NewRPC(c, c.HandlerRegistry, c.logger)
	bus.RegisterConfigReload(c, c.HandlerRegistry)
	bus.RegisterClusterStats(c, c.rpc)
	bus.RegisterWebConnCount(c, c.rpc)
	bus.RegisterPluginStatuses(c, c.rpc)

	return c, nil
}

// NodeID exposes the resolved node identifier for logs and tests.
func (c *Cluster) NodeID() string { return c.node.ID }

// Log satisfies bus.P2Host by surfacing the cluster's logger to shared helpers.
func (c *Cluster) Log() mlog.LoggerIFace { return c.logger }

// ReloadConfig satisfies bus.P2Host by delegating to the upstream
// PlatformService. Wrapping through Cluster keeps bus/ decoupled from the
// concrete platform type.
func (c *Cluster) ReloadConfig() error { return c.ps.ReloadConfig() }

// LocalWebConnCountForUser is the responder-side accessor used by the
// webconn-count RPC; it must NOT recurse into the cluster-wide
// WebConnCountForUser exposed on ClusterInterface.
func (c *Cluster) LocalWebConnCountForUser(userID string) int {
	return c.ps.WebConnCountForUser(userID)
}

// LocalPluginStatuses is the responder-side accessor for the plugin-statuses
// RPC; see LocalWebConnCountForUser for the non-recursion rationale.
func (c *Cluster) LocalPluginStatuses() (model.PluginStatuses, *model.AppError) {
	return c.ps.GetPluginStatuses()
}

// --- ClusterInterface: lifecycle ------------------------------------------

func (c *Cluster) StartInterNodeCommunication() {
	if !c.started.CompareAndSwap(false, true) {
		return
	}
	c.cancelCtx, c.cancelFn = context.WithCancel(context.Background())

	if err := c.startListener(); err != nil {
		// Could not even open LISTEN. Undo the start flag so StopInterNode
		// stays a no-op for callers, and log loudly. Server continues to run
		// single-node.
		c.logger.Error("Failed to start postgres cluster listener; HA disabled",
			mlog.Err(err))
		c.cancelFn()
		c.started.Store(false)
		return
	}

	// P2 RPC handlers are already wired in New(); see the comment there
	// for the rationale (avoids nil rpc during Channels().Start() plugin
	// bootstrap which calls into GetPluginStatuses before Start runs).

	c.startHeartbeat()

	queueSize := c.opts.SendQueueSize
	if queueSize <= 0 {
		queueSize = 8192
	}
	workers := c.opts.SendWorkers
	if workers <= 0 {
		workers = 8
	}
	// Publish sendCh under the lock so enqueueSend either sees nil
	// (pre-Start fallback to directSend) or the fully-constructed channel.
	sendCh := make(chan sendTask, queueSize)
	c.sendMu.Lock()
	c.sendCh = sendCh
	c.sendMu.Unlock()
	for i := 0; i < workers; i++ {
		c.sendWg.Add(1)
		go c.sendWorker(sendCh)
	}

	c.wg.Add(3)
	go c.listenerLoop()
	go c.leaderLoop()
	go c.gcLoop()

	c.logger.Info("Postgres community cluster started",
		mlog.String("node_id", c.node.ID),
		mlog.String("channel", c.opts.ChannelName),
		mlog.String("cluster_name", c.node.ClusterName),
		mlog.Int("send_queue_size", queueSize),
		mlog.Int("send_workers", workers))
}

func (c *Cluster) StopInterNodeCommunication() {
	if !c.started.CompareAndSwap(true, false) {
		return
	}
	c.cancelFn()

	// Order matters: close the listener first so listenerLoop's select can
	// exit via its Notify-closed branch; then stop the heartbeat (which
	// blocks on a channel send inside the upstream discovery service).
	if c.listener != nil {
		if err := c.listener.Close(); err != nil {
			c.logger.Warn("Failed to close postgres listener", mlog.Err(err))
		}
	}
	c.stopHeartbeat()

	// Workers exit on cancelCtx.Done() (already triggered above) after
	// draining whatever is left in sendCh. We deliberately don't close
	// sendCh — that would race with concurrent enqueueSend in the
	// select-with-default and panic. Leaving the channel open lets
	// post-Stop SendClusterMessage calls fill the buffer harmlessly until
	// process exit reclaims the memory.
	c.sendWg.Wait()

	c.wg.Wait()

	if dropped := c.sendDropped.Load(); dropped > 0 {
		c.logger.Warn("Postgres cluster shed messages while queue was saturated",
			mlog.Int("dropped_total", dropped))
	}

	if err := c.db.Close(); err != nil {
		c.logger.Warn("Failed to close postgres cluster db pool", mlog.Err(err))
	}

	c.logger.Info("Postgres community cluster stopped",
		mlog.String("node_id", c.node.ID))
}

// --- ClusterInterface: identity -------------------------------------------

func (c *Cluster) GetClusterId() string { return c.node.ClusterName }

func (c *Cluster) IsLeader() bool { return c.leaderCache.Load() }

func (c *Cluster) HealthScore() int { return c.health.Score() }

func (c *Cluster) GetMyClusterInfo() *model.ClusterInfo {
	return &model.ClusterInfo{
		Id:        c.node.ID,
		Hostname:  c.node.Hostname,
		IPAddress: c.node.IPAddress,
		Version:   model.CurrentVersion,
	}
}

// GetClusterInfos lists every peer that has pinged the cluster_discovery
// table within CDSOfflineAfterMillis. Returns our own row plus all known
// peers; the admin UI decides how to render self-vs-peer.
func (c *Cluster) GetClusterInfos() ([]*model.ClusterInfo, error) {
	rows, err := c.listActiveNodes()
	if err != nil {
		return nil, err
	}
	infos := make([]*model.ClusterInfo, 0, len(rows))
	for _, r := range rows {
		infos = append(infos, &model.ClusterInfo{
			Id:       r.Id,
			Hostname: r.Hostname,
			Version:  model.CurrentVersion,
		})
	}
	return infos, nil
}

// --- ClusterInterface: send -----------------------------------------------
//
// SendClusterMessage and SendClusterMessageToNode are asynchronous: encoding
// happens on the caller goroutine, then the payload is enqueued onto sendCh.
// A pool of sendWorker goroutines drains the queue into PostgreSQL. This
// design keeps the WebSocket broadcast hot-path free of DB latency, and bounds
// PG connection pressure to the worker count instead of the caller count.
//
// When the queue is full, messages are dropped rather than blocking — the
// ClusterMessage.SendType=ClusterSendBestEffort semantics that Mattermost
// uses for most events explicitly allows this. The dropped counter is logged
// periodically so operators can detect sustained overload.

func (c *Cluster) SendClusterMessage(msg *model.ClusterMessage) {
	c.enqueueSend("", msg)
}

func (c *Cluster) SendClusterMessageToNode(nodeID string, msg *model.ClusterMessage) error {
	if nodeID == "" {
		return fmt.Errorf("postgres cluster: empty target node ID")
	}
	if !c.enqueueSend(nodeID, msg) {
		return fmt.Errorf("postgres cluster: send queue full")
	}
	return nil
}

// enqueueSend encodes the envelope and tries to push it onto sendCh. Returns
// false when the queue is full or the cluster has not been started yet.
//
// The RLock around the channel read is purely a memory-visibility fence with
// the Start-side Lock — it does NOT guard against close, because sendCh is
// never closed (see the comment on the Cluster struct).
func (c *Cluster) enqueueSend(target string, msg *model.ClusterMessage) bool {
	payload, err := bus.Encode(c.node.ID, target, msg)
	if err != nil {
		c.logger.Warn("Failed to encode cluster envelope",
			mlog.String("event", string(msg.Event)),
			mlog.Err(err))
		return false
	}

	c.sendMu.RLock()
	sendCh := c.sendCh
	c.sendMu.RUnlock()

	if sendCh == nil {
		// StartInterNodeCommunication hasn't run yet (typically because a
		// plugin loaded during Channels().Start() is calling into cluster
		// methods before the worker pool exists). Fall back to a direct
		// synchronous send — c.db is already initialized by New().
		return c.directSend(target, payload) == nil
	}

	select {
	case sendCh <- sendTask{target: target, payload: payload}:
		return true
	default:
		dropped := c.sendDropped.Add(1)
		c.health.RecordFailure()
		// Throttle the warning: one in every 1000 drops is enough to surface
		// the condition without flooding logs during sustained overload.
		if dropped == 1 || dropped%1000 == 0 {
			c.logger.Warn("Postgres cluster send queue full; dropping message",
				mlog.String("event", string(msg.Event)),
				mlog.Int("dropped_total", dropped))
		}
		return false
	}
}

// sendWorker drains sendCh into PostgreSQL. Each worker briefly holds one
// pool connection per message via insertAndNotify (now a single-statement
// CTE — see storage.go), so concurrent in-flight DB work is bounded by the
// worker count, not by the caller count.
//
// The worker takes sendCh as an argument rather than reading c.sendCh so
// the captured reference is unambiguous (Start always passes the same
// channel it just assigned to c.sendCh). On Stop, cancelCtx.Done() fires,
// the worker drains whatever is left in the channel non-blockingly, then
// returns. After all workers return, sendWg unblocks Stop.
func (c *Cluster) sendWorker(sendCh <-chan sendTask) {
	defer c.sendWg.Done()
	for {
		select {
		case task := <-sendCh:
			c.runSendTask(task)
		case <-c.cancelCtx.Done():
			// Drain anything still queued so a graceful Stop doesn't drop
			// already-accepted messages. Items enqueued *after* this drain
			// completes are lost — acceptable for best-effort cluster
			// broadcasts and far better than panicking on close.
			for {
				select {
				case task := <-sendCh:
					c.runSendTask(task)
				default:
					return
				}
			}
		}
	}
}

func (c *Cluster) runSendTask(task sendTask) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.insertAndNotify(ctx, model.NewId(), task.target, task.payload, nowMS()); err != nil {
		c.health.RecordFailure()
		c.logger.Error("Failed to publish postgres cluster message", mlog.Err(err))
		return
	}
	c.health.RecordSuccess()
}

// directSend is the pre-Start fallback used when a plugin or boot-time
// handler emits a cluster message before the worker pool is up. It performs
// the same single-statement INSERT+NOTIFY synchronously on the caller
// goroutine.
func (c *Cluster) directSend(target string, payload []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.insertAndNotify(ctx, model.NewId(), target, payload, nowMS())
}

// NotifyMsg allows external code to inject raw envelope bytes into the
// dispatch pipeline. Symmetric with the Redis backend; mostly used by tests.
func (c *Cluster) NotifyMsg(buf []byte) {
	env, err := bus.Decode(buf)
	if err != nil {
		c.logger.Warn("Failed to decode cluster envelope", mlog.Err(err))
		return
	}
	if env.Sender == c.node.ID || env.Message == nil {
		return
	}
	if env.Target != "" && env.Target != c.node.ID {
		return
	}
	c.Dispatch(env.Message)
}

// --- ClusterInterface: P2 stubs -------------------------------------------
//
// Kept intentionally minimal in Phase 2. Filling them in does not affect
// hot-path semantics; upstream treats them as advisory.

// LocalStats builds this node's ClusterStats without any network calls.
func (c *Cluster) LocalStats() *model.ClusterStats {
	return &model.ClusterStats{
		Id:                        c.node.ID,
		TotalWebsocketConnections: c.ps.TotalWebsocketConnections(),
	}
}

// GetClusterStats aggregates LocalStats from every peer via the shared RPC
// helper. Local stats are always included; peers that time out are absent
// from the returned slice.
func (c *Cluster) GetClusterStats(rctx request.CTX) ([]*model.ClusterStats, *model.AppError) {
	peers, err := c.GetClusterInfos()
	expected := 0
	if err == nil {
		expected = len(peers) - 1
	}
	if expected < 0 {
		expected = 0
	}
	return bus.CollectClusterStats(c, c.rpc, expected, 2*time.Second)
}

func (c *Cluster) GetLogs(rctx request.CTX, page, perPage int) ([]string, *model.AppError) {
	return nil, nil
}

func (c *Cluster) QueryLogs(rctx request.CTX, page, perPage int) (map[string][]string, *model.AppError) {
	return nil, nil
}

func (c *Cluster) GenerateSupportPacket(rctx request.CTX, options *model.SupportPacketOptions) (map[string][]model.FileData, error) {
	return nil, nil
}

// GetPluginStatuses aggregates plugin statuses from every peer; see the
// equivalent docstring in redis/cluster.go for the full rationale.
func (c *Cluster) GetPluginStatuses() (model.PluginStatuses, *model.AppError) {
	peers, err := c.GetClusterInfos()
	expected := 0
	if err == nil {
		expected = len(peers) - 1
	}
	if expected < 0 {
		expected = 0
	}
	return bus.CollectPluginStatuses(c, c.rpc, expected, 2*time.Second)
}

// ConfigChanged broadcasts a lightweight "reload" signal; peers re-read the
// shared config store on receipt. See redis.Cluster.ConfigChanged for the
// full rationale — behavior is identical across backends.
func (c *Cluster) ConfigChanged(previousConfig, newConfig *model.Config, sendToOtherServer bool) *model.AppError {
	return bus.SendConfigReload(c, sendToOtherServer)
}

// WebConnCountForUser returns the active websocket connection count for
// userID across all peers (excluding self); see the redis counterpart for
// the full rationale on gating "mark user offline" correctness in HA.
func (c *Cluster) WebConnCountForUser(userID string) (int, *model.AppError) {
	peers, err := c.GetClusterInfos()
	expected := 0
	if err == nil {
		expected = len(peers) - 1
	}
	if expected < 0 {
		expected = 0
	}
	timeout := c.opts.WebConnRPCTimeout
	if timeout <= 0 {
		timeout = defaultWebConnRPCTimeout
	}
	return bus.CollectWebConnCount(c, c.rpc, userID, expected, timeout)
}

func (c *Cluster) GetWSQueues(userID, connectionID string, seqNum int64) (map[string]*model.WSQueues, error) {
	return nil, nil
}

// nowMS returns wall-clock milliseconds since the unix epoch. Defined here
// so both cluster.go and storage.go share a single clock source.
func nowMS() int64 { return time.Now().UnixMilli() }
