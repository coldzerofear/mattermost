// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package redis

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/rueidis"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/shared/mlog"
	"github.com/mattermost/mattermost/server/public/shared/request"
	"github.com/mattermost/mattermost/server/v8/channels/app/platform/clustercommunity/bus"
)

// platformDeps is the subset of *platform.PlatformService that Cluster uses.
// Declaring it as a local interface lets tests provide a fake instead of
// standing up a full PlatformService (which requires a DB, config store, etc.).
// *platform.PlatformService satisfies this automatically — production code
// does not need any adapter.
type platformDeps interface {
	Log() mlog.LoggerIFace
	InvokeClusterLeaderChangedListeners()
	// ReloadConfig is called by the ClusterEventCommunityConfigReload handler
	// when a peer signals that the shared config store was updated.
	ReloadConfig() error
	// TotalWebsocketConnections feeds GetClusterStats; each node reports its
	// own count so the System Console can render a per-node breakdown.
	TotalWebsocketConnections() int
	// WebConnCountForUser returns the number of active websocket connections
	// on THIS node for the given user. Drives the per-user presence check
	// that gates "mark user offline" in the hub.
	WebConnCountForUser(userID string) int
	// GetPluginStatuses returns the plugin statuses registered on THIS node.
	// Upstream *PlatformService.GetPluginStatuses already has cluster-aware
	// semantics by convention (it is meant to be called by cluster
	// implementations, per its own comment), so pulling it through the
	// interface is safe.
	GetPluginStatuses() (model.PluginStatuses, *model.AppError)
}

// Cluster is the Redis-backed implementation of einterfaces.ClusterInterface.
//
// Goroutines started by StartInterNodeCommunication:
//   - subscribeLoop: blocks on rueidis.Receive for broadcast + node-direct channels.
//   - heartbeatLoop: periodically writes mm:cluster:nodes:<id> with a TTL.
//   - leaderLoop:    periodically SCANs all node keys and caches "is leader".
//
// All three share cancelCtx; StopInterNodeCommunication cancels it and waits.
type Cluster struct {
	*bus.HandlerRegistry

	ps     platformDeps
	opts   *Options
	node   *bus.NodeIdentity
	logger mlog.LoggerIFace

	// Separate clients:
	//   cmdClient receives short-lived commands (PUBLISH / SET / SCAN).
	//   subClient is dedicated to SUBSCRIBE; rueidis.Receive holds the connection
	//   until the subscription ends, so sharing would block all other commands.
	cmdClient rueidis.Client
	subClient rueidis.Client

	cancelCtx context.Context
	cancelFn  context.CancelFunc
	wg        sync.WaitGroup

	leaderCache atomic.Bool
	started     atomic.Bool
	health      *bus.HealthTracker

	// rpc handles request/response flows for P2 methods (GetClusterStats).
	// Initialized lazily so tests that don't call StartInterNodeCommunication
	// still see a usable Cluster.
	rpc *bus.RPC
}

// New constructs a Cluster ready to be returned from the factory. Start is not
// called here; StartInterNodeCommunication is invoked later by Server.Start
// after handlers have been registered.
//
// Returns an error if the Redis address is missing or if initial connection
// setup fails; the factory then logs and returns nil to fall back to
// single-node mode, rather than crashing the server.
func New(ps platformDeps, opts *Options) (*Cluster, error) {
	if opts.Addr == "" {
		return nil, errors.New("MM_CLUSTER_REDIS_ADDR is required when MM_CLUSTER_MODE=redis")
	}

	node := bus.ResolveNodeIdentity(opts.NodeID, opts.ClusterName)

	cmd, err := newRueidisClient(opts)
	if err != nil {
		return nil, err
	}
	sub, err := newRueidisClient(opts)
	if err != nil {
		cmd.Close()
		return nil, err
	}

	c := &Cluster{
		HandlerRegistry: bus.NewHandlerRegistry(),
		ps:              ps,
		opts:            opts,
		node:            node,
		logger:          ps.Log(),
		cmdClient:       cmd,
		subClient:       sub,
		health:          &bus.HealthTracker{},
	}

	// Initialize cancelCtx *here*, not in StartInterNodeCommunication, for the
	// same reason the RPC wiring below is done here: Channels().Start() runs
	// before StartInterNodeCommunication, and plugin initialization during that
	// window calls GetPluginStatuses -> GetClusterInfos -> scanAllNodeIDs, all
	// of which read c.cancelCtx. If it were still nil, context.WithTimeout would
	// panic with "cannot create context from nil parent". StartInterNodeCommunication
	// re-creates it on (re)start, so restart-after-stop semantics are unchanged.
	c.cancelCtx, c.cancelFn = context.WithCancel(context.Background())

	// Wire the P2 RPC infrastructure and register handlers *here*, not in
	// StartInterNodeCommunication. Reason: the upstream Server calls
	// Channels().Start() before StartInterNodeCommunication, and plugin
	// initialization during that window can trigger GetPluginStatuses /
	// notifyPluginStatusesChanged, which dispatch into c.rpc.Broadcast.
	// If c.rpc were nil at that point, we'd crash with a nil-pointer
	// dereference. Handlers being registered before the subscribe loop
	// runs is harmless — dispatch is driven by that loop, so nothing
	// fires until StartInterNodeCommunication anyway.
	c.rpc = bus.NewRPC(c, c.HandlerRegistry, c.logger)
	bus.RegisterConfigReload(c, c.HandlerRegistry)
	bus.RegisterClusterStats(c, c.rpc)
	bus.RegisterWebConnCount(c, c.rpc)
	bus.RegisterPluginStatuses(c, c.rpc)

	return c, nil
}

// newRueidisClient is the single place we construct rueidis clients so both
// cmd and sub connections share identical dial options.
//
// ForceSingleClient: true because cluster.go uses a single Redis instance;
// a rueidis sentinel/cluster topology is a separate concern outside this MVP.
func newRueidisClient(opts *Options) (rueidis.Client, error) {
	copt := rueidis.ClientOption{
		InitAddress:       []string{opts.Addr},
		Password:          opts.Password,
		SelectDB:          opts.DB,
		ForceSingleClient: true,
		// Disable client-side caching: we do not issue cacheable reads.
		DisableCache: true,
	}
	if opts.TLS {
		copt.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return rueidis.NewClient(copt)
}

// NodeID exposes the resolved node identifier for logs and tests. Not part of
// ClusterInterface.
func (c *Cluster) NodeID() string { return c.node.ID }

// Log satisfies bus.P2Host by surfacing the cluster's logger to shared helpers.
func (c *Cluster) Log() mlog.LoggerIFace { return c.logger }

// ReloadConfig satisfies bus.P2Host; it simply delegates to the upstream
// PlatformService. Wrapping through Cluster keeps bus/ free of any direct
// dependency on *platform.PlatformService.
func (c *Cluster) ReloadConfig() error { return c.ps.ReloadConfig() }

// LocalWebConnCountForUser is the responder-side accessor for the
// webconn-count RPC. It MUST NOT recurse into Cluster.WebConnCountForUser
// (which is the aggregator); going directly to the platformDeps keeps the
// two separate.
func (c *Cluster) LocalWebConnCountForUser(userID string) int {
	return c.ps.WebConnCountForUser(userID)
}

// LocalPluginStatuses is the responder-side accessor for the plugin-statuses
// RPC. See LocalWebConnCountForUser above for why this indirection exists.
func (c *Cluster) LocalPluginStatuses() (model.PluginStatuses, *model.AppError) {
	return c.ps.GetPluginStatuses()
}

// --- ClusterInterface: lifecycle ------------------------------------------

func (c *Cluster) StartInterNodeCommunication() {
	if !c.started.CompareAndSwap(false, true) {
		return
	}
	c.cancelCtx, c.cancelFn = context.WithCancel(context.Background())

	// P2 RPC handlers are already wired in New(); see the comment there
	// for the rationale (avoids nil rpc during Channels().Start() plugin
	// bootstrap which calls into GetPluginStatuses before Start runs).

	// Write the first heartbeat synchronously so other nodes see us
	// immediately; without this, the first external IsLeader check can
	// race against node startup.
	if err := c.writeHeartbeat(c.cancelCtx); err != nil {
		c.logger.Warn("Initial cluster heartbeat failed",
			mlog.String("node_id", c.node.ID),
			mlog.Err(err))
	}

	c.wg.Add(3)
	go c.subscribeLoop()
	go c.heartbeatLoop()
	go c.leaderLoop()

	c.logger.Info("Community cluster started",
		mlog.String("node_id", c.node.ID),
		mlog.String("cluster_name", c.node.ClusterName))
}

func (c *Cluster) StopInterNodeCommunication() {
	if !c.started.CompareAndSwap(true, false) {
		return
	}
	c.cancelFn()

	// Best-effort: remove our heartbeat key so peers stop considering us
	// alive without waiting for TTL expiry.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.cmdClient.Do(ctx, c.cmdClient.B().Del().Key(c.heartbeatKey()).Build()).Error(); err != nil {
		c.logger.Warn("Failed to delete heartbeat key on shutdown", mlog.Err(err))
	}

	c.wg.Wait()
	c.cmdClient.Close()
	c.subClient.Close()

	c.logger.Info("Community cluster stopped", mlog.String("node_id", c.node.ID))
}

// --- ClusterInterface: identity -------------------------------------------

// GetClusterId returns the logical cluster name (shared across all replicas),
// not the per-node ID. Upstream enterprise uses the same semantics.
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

// --- ClusterInterface: stubs for P2 methods -------------------------------
//
// These are intentionally minimal in Phase 1. Filling them in later does not
// change the hot-path behavior of the server. Returning nil / zero values is
// safe: upstream code treats these as advisory outputs.

// LocalStats builds this node's ClusterStats without any network calls. It is
// called both as the local entry of GetClusterStats and as the responder for
// peer stats requests (see bus/p2.go).
func (c *Cluster) LocalStats() *model.ClusterStats {
	return &model.ClusterStats{
		Id:                        c.node.ID,
		TotalWebsocketConnections: c.ps.TotalWebsocketConnections(),
		// DB connection counts are tracked by the Mattermost store layer, not
		// the cluster. Leaving them at zero is accurate (clustercommunity does
		// not open its own DB connections on the Redis path) and matches
		// behavior for any peer that hasn't wired those metrics.
	}
}

// GetClusterStats aggregates LocalStats from every peer via the shared RPC
// helper. The return always includes this node's stats; peers that fail to
// respond within the timeout are simply absent from the slice.
func (c *Cluster) GetClusterStats(rctx request.CTX) ([]*model.ClusterStats, *model.AppError) {
	// Cap the expected-peer count at the number of live nodes minus self.
	// GetClusterInfos may return stale entries if Redis just evicted a key;
	// the RPC helper's timeout bounds the wait regardless.
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

// GetPluginStatuses aggregates plugin statuses from every peer. The caller
// (channels/app/plugin_statuses.go) merges this with its own local
// statuses, so we do NOT include self-data here — sending it would produce
// duplicated entries in the System Console UI.
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

// ConfigChanged broadcasts a lightweight "reload" signal whenever an admin
// saves new settings. The new config itself is persisted to the shared config
// store by the caller (platform.saveConfig); peers re-read it by calling
// ReloadConfig on receipt of ClusterEventCommunityConfigReload.
//
// previousConfig and newConfig are retained in the interface signature to
// match upstream enterprise contract, but are not transmitted: sending them
// would risk rewriting the shared store out-of-order and balloon message
// size.
func (c *Cluster) ConfigChanged(previousConfig, newConfig *model.Config, sendToOtherServer bool) *model.AppError {
	return bus.SendConfigReload(c, sendToOtherServer)
}

// WebConnCountForUser returns the number of active websocket connections
// the given user holds across ALL OTHER nodes. The caller in
// platform/web_hub.go gates "mark user offline" on this being zero; it
// combines this with its own local count separately, so we deliberately
// exclude self from the result.
//
// Timeout is tight (1s) because this runs in the hot path of the hub's
// inactive-connection reaper; delaying that would defer presence updates
// for every user whose last socket just closed.
func (c *Cluster) WebConnCountForUser(userID string) (int, *model.AppError) {
	peers, err := c.GetClusterInfos()
	expected := 0
	if err == nil {
		expected = len(peers) - 1
	}
	if expected < 0 {
		expected = 0
	}
	return bus.CollectWebConnCount(c, c.rpc, userID, expected, time.Second)
}

func (c *Cluster) GetWSQueues(userID, connectionID string, seqNum int64) (map[string]*model.WSQueues, error) {
	return nil, nil
}
