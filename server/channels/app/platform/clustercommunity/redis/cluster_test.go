// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package redis

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/shared/mlog"
)

// fakePlatform is a minimal platformDeps stand-in. It satisfies the narrow
// interface Cluster needs without dragging in the whole PlatformService
// infrastructure (DB, config, logging subsystem, plugin loader, ...). The
// tests only need to count listener invocations and receive a working logger.
type fakePlatform struct {
	logger             *mlog.Logger
	leaderChangedCount atomic.Int32
	reloadCount        atomic.Int32
	reloadErr          error
	wsConnections      atomic.Int64

	// userConnMu protects userConnCounts; tests mutate per-user counts
	// between assertions so the race detector would flag plain-map access.
	userConnMu     sync.Mutex
	userConnCounts map[string]int

	// pluginStatusesMu protects pluginStatuses; same rationale.
	pluginStatusesMu sync.Mutex
	pluginStatuses   model.PluginStatuses
}

func newFakePlatform(t *testing.T) *fakePlatform {
	return &fakePlatform{logger: mlog.CreateConsoleTestLogger(t)}
}

func (f *fakePlatform) Log() mlog.LoggerIFace { return f.logger }

func (f *fakePlatform) InvokeClusterLeaderChangedListeners() {
	f.leaderChangedCount.Add(1)
}

func (f *fakePlatform) ReloadConfig() error {
	f.reloadCount.Add(1)
	return f.reloadErr
}

func (f *fakePlatform) TotalWebsocketConnections() int {
	return int(f.wsConnections.Load())
}

func (f *fakePlatform) WebConnCountForUser(userID string) int {
	f.userConnMu.Lock()
	defer f.userConnMu.Unlock()
	return f.userConnCounts[userID]
}

func (f *fakePlatform) setUserConnCount(userID string, n int) {
	f.userConnMu.Lock()
	defer f.userConnMu.Unlock()
	if f.userConnCounts == nil {
		f.userConnCounts = map[string]int{}
	}
	f.userConnCounts[userID] = n
}

func (f *fakePlatform) GetPluginStatuses() (model.PluginStatuses, *model.AppError) {
	f.pluginStatusesMu.Lock()
	defer f.pluginStatusesMu.Unlock()
	// Return a copy so callers can't mutate our internal slice concurrently
	// with setter calls from other goroutines.
	out := make(model.PluginStatuses, len(f.pluginStatuses))
	copy(out, f.pluginStatuses)
	return out, nil
}

func (f *fakePlatform) setPluginStatuses(ps model.PluginStatuses) {
	f.pluginStatusesMu.Lock()
	defer f.pluginStatusesMu.Unlock()
	f.pluginStatuses = ps
}

// newTestCluster spins up a Cluster pointed at the given miniredis address.
// The caller is responsible for StopInterNodeCommunication in test cleanup.
//
// Each invocation gets its own *fakePlatform so listener counts don't leak
// between tests running in parallel.
func newTestCluster(t *testing.T, addr, nodeID string) (*Cluster, *fakePlatform) {
	t.Helper()

	fake := newFakePlatform(t)
	c, err := New(fake, &Options{
		NodeID:            nodeID,
		ClusterName:       "test-cluster",
		HeartbeatInterval: 200 * time.Millisecond,
		HeartbeatTTL:      2 * time.Second,
		Addr:              addr,
		KeyPrefix:         "mm:cluster:test",
	})
	require.NoError(t, err)
	return c, fake
}

// eventuallyTrue polls cond at 20ms intervals and fails the test if the
// condition does not become true within the timeout. Useful for
// background-goroutine assertions without introducing flaky sleeps.
func eventuallyTrue(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition never became true within %v: %s", timeout, msg)
}

func TestRedisClusterSendReceiveBroadcast(t *testing.T) {
	mr := miniredis.RunT(t)

	sender, _ := newTestCluster(t, mr.Addr(), "node-sender")
	receiver, _ := newTestCluster(t, mr.Addr(), "node-receiver")
	sender.StartInterNodeCommunication()
	receiver.StartInterNodeCommunication()
	defer sender.StopInterNodeCommunication()
	defer receiver.StopInterNodeCommunication()

	// Register a handler on the receiver BEFORE the sender publishes. In
	// real use, handlers are registered during server startup before
	// StartInterNodeCommunication, but either order is valid; the registry
	// is safe for concurrent Register/Dispatch.
	ch := make(chan *model.ClusterMessage, 1)
	receiver.RegisterClusterMessageHandler(model.ClusterEventPublish, func(msg *model.ClusterMessage) {
		ch <- msg
	})

	// Wait for the subscriber to have actually SUBSCRIBEd. If we publish
	// too early the message is dropped silently (Redis pub/sub is fire-and-
	// forget). miniredis exposes this via Server.Subscribers().
	eventuallyTrue(t, 2*time.Second, func() bool {
		return len(mr.PubSubChannels(sender.broadcastChannel())) >= 1
	}, "subscriber never joined broadcast channel")

	sender.SendClusterMessage(&model.ClusterMessage{
		Event: model.ClusterEventPublish,
		Data:  []byte("hello"),
	})

	select {
	case got := <-ch:
		require.Equal(t, model.ClusterEventPublish, got.Event)
		require.Equal(t, []byte("hello"), got.Data)
	case <-time.After(3 * time.Second):
		t.Fatal("receiver did not get the broadcast within 3s")
	}
}

func TestRedisClusterLoopbackFiltered(t *testing.T) {
	// The sender must not receive its own broadcast even though it
	// subscribes to the same channel.
	mr := miniredis.RunT(t)
	c, _ := newTestCluster(t, mr.Addr(), "node-self")
	c.StartInterNodeCommunication()
	defer c.StopInterNodeCommunication()

	delivered := make(chan struct{}, 1)
	c.RegisterClusterMessageHandler(model.ClusterEventPublish, func(*model.ClusterMessage) {
		delivered <- struct{}{}
	})

	eventuallyTrue(t, 2*time.Second, func() bool {
		return len(mr.PubSubChannels(c.broadcastChannel())) >= 1
	}, "self never subscribed")

	c.SendClusterMessage(&model.ClusterMessage{Event: model.ClusterEventPublish, Data: []byte("x")})

	select {
	case <-delivered:
		t.Fatal("sender received its own broadcast (loopback filter broken)")
	case <-time.After(300 * time.Millisecond):
		// expected: filter worked
	}
}

func TestRedisClusterDirectMessage(t *testing.T) {
	mr := miniredis.RunT(t)

	a, _ := newTestCluster(t, mr.Addr(), "node-a")
	b, _ := newTestCluster(t, mr.Addr(), "node-b")
	c, _ := newTestCluster(t, mr.Addr(), "node-c")
	a.StartInterNodeCommunication()
	b.StartInterNodeCommunication()
	c.StartInterNodeCommunication()
	defer a.StopInterNodeCommunication()
	defer b.StopInterNodeCommunication()
	defer c.StopInterNodeCommunication()

	bGot := make(chan struct{}, 1)
	cGot := make(chan struct{}, 1)
	b.RegisterClusterMessageHandler("direct", func(*model.ClusterMessage) { bGot <- struct{}{} })
	c.RegisterClusterMessageHandler("direct", func(*model.ClusterMessage) { cGot <- struct{}{} })

	// Wait for both B and C to subscribe to their own node channels.
	eventuallyTrue(t, 2*time.Second, func() bool {
		return len(mr.PubSubChannels(b.nodeChannel("node-b"))) >= 1 &&
			len(mr.PubSubChannels(c.nodeChannel("node-c"))) >= 1
	}, "node-b / node-c never subscribed to their direct channels")

	require.NoError(t, a.SendClusterMessageToNode("node-b", &model.ClusterMessage{Event: "direct"}))

	select {
	case <-bGot:
		// expected
	case <-time.After(2 * time.Second):
		t.Fatal("direct target did not receive the message")
	}

	// C must NOT have received it — directed delivery is not broadcast.
	select {
	case <-cGot:
		t.Fatal("non-target node received a direct message")
	case <-time.After(150 * time.Millisecond):
	}
}

func TestRedisClusterLeaderElection(t *testing.T) {
	mr := miniredis.RunT(t)

	// IDs deliberately chosen so that "node-a" < "node-b" < "node-c"
	// lexicographically, pinning the expected leader.
	a, faA := newTestCluster(t, mr.Addr(), "node-a")
	b, _ := newTestCluster(t, mr.Addr(), "node-b")
	c, _ := newTestCluster(t, mr.Addr(), "node-c")

	// Start b and c first, then a. The correct leader is still a even
	// though it started last — the election is based on ID, not join time.
	b.StartInterNodeCommunication()
	c.StartInterNodeCommunication()
	a.StartInterNodeCommunication()
	defer a.StopInterNodeCommunication()
	defer b.StopInterNodeCommunication()
	defer c.StopInterNodeCommunication()

	eventuallyTrue(t, 10*time.Second, func() bool {
		return a.IsLeader() && !b.IsLeader() && !c.IsLeader()
	}, "expected node-a to be sole leader")

	// At least one leader-change listener fire occurred on a (it
	// transitioned false -> true).
	require.GreaterOrEqual(t, faA.leaderChangedCount.Load(), int32(1),
		"leader listener should have fired on the new leader")
}

func TestRedisClusterNoLeaderWithoutRedis(t *testing.T) {
	// If Redis dies, the SCAN fails and recomputeLeader must force the
	// cache to false. Verifying this prevents the scenario where a split
	// brain Redis outage leaves two replicas both believing they are leader.
	mr := miniredis.RunT(t)
	a, _ := newTestCluster(t, mr.Addr(), "node-a")
	a.StartInterNodeCommunication()
	defer a.StopInterNodeCommunication()

	eventuallyTrue(t, 10*time.Second, func() bool {
		return a.IsLeader()
	}, "node-a should become leader with a single-node cluster")

	mr.Close()

	eventuallyTrue(t, 10*time.Second, func() bool {
		return !a.IsLeader()
	}, "IsLeader must go to false when Redis is unreachable")
}

func TestRedisConfigChangedBroadcastsReload(t *testing.T) {
	// A ConfigChanged on one node must cause every peer to call
	// ReloadConfig. The sender itself must NOT reload — it already has the
	// new value locally.
	mr := miniredis.RunT(t)

	sender, senderFake := newTestCluster(t, mr.Addr(), "node-saver")
	peer1, peer1Fake := newTestCluster(t, mr.Addr(), "node-peer-1")
	peer2, peer2Fake := newTestCluster(t, mr.Addr(), "node-peer-2")
	sender.StartInterNodeCommunication()
	peer1.StartInterNodeCommunication()
	peer2.StartInterNodeCommunication()
	defer sender.StopInterNodeCommunication()
	defer peer1.StopInterNodeCommunication()
	defer peer2.StopInterNodeCommunication()

	eventuallyTrue(t, 2*time.Second, func() bool {
		return mr.PubSubNumSub(sender.broadcastChannel())[sender.broadcastChannel()] >= 3
	}, "all three nodes must join the broadcast channel")

	appErr := sender.ConfigChanged(nil, nil, true)
	require.Nil(t, appErr)

	eventuallyTrue(t, 2*time.Second, func() bool {
		return peer1Fake.reloadCount.Load() == 1 && peer2Fake.reloadCount.Load() == 1
	}, "both peers should have reloaded their config exactly once")

	require.Equal(t, int32(0), senderFake.reloadCount.Load(),
		"sender must not reload its own config (loopback filter)")
}

func TestRedisConfigChangedSkipBroadcast(t *testing.T) {
	// sendToOtherServer=false is the signal from the saveConfig path that
	// this change should stay local (e.g. a config hot-reload test path).
	// ConfigChanged must not publish anything in that case.
	mr := miniredis.RunT(t)

	sender, _ := newTestCluster(t, mr.Addr(), "node-saver")
	peer, peerFake := newTestCluster(t, mr.Addr(), "node-peer")
	sender.StartInterNodeCommunication()
	peer.StartInterNodeCommunication()
	defer sender.StopInterNodeCommunication()
	defer peer.StopInterNodeCommunication()

	eventuallyTrue(t, 2*time.Second, func() bool {
		return mr.PubSubNumSub(sender.broadcastChannel())[sender.broadcastChannel()] >= 2
	}, "peer must have joined the broadcast channel")

	appErr := sender.ConfigChanged(nil, nil, false)
	require.Nil(t, appErr)

	// Wait a bit so any inadvertent broadcast would have propagated.
	time.Sleep(300 * time.Millisecond)
	require.Equal(t, int32(0), peerFake.reloadCount.Load(),
		"peer must not reload when sendToOtherServer=false")
}

func TestRedisGetClusterStatsAggregates(t *testing.T) {
	// Set each node's fake websocket count to a unique value; GetClusterStats
	// must return one entry per responding node, each carrying that node's
	// own count. This verifies both the responder side (peer replies with
	// its own LocalStats) and the collector side (requester fans out and
	// collects).
	mr := miniredis.RunT(t)

	a, aFake := newTestCluster(t, mr.Addr(), "node-stats-a")
	b, bFake := newTestCluster(t, mr.Addr(), "node-stats-b")
	cc, ccFake := newTestCluster(t, mr.Addr(), "node-stats-c")
	a.StartInterNodeCommunication()
	b.StartInterNodeCommunication()
	cc.StartInterNodeCommunication()
	defer a.StopInterNodeCommunication()
	defer b.StopInterNodeCommunication()
	defer cc.StopInterNodeCommunication()

	aFake.wsConnections.Store(11)
	bFake.wsConnections.Store(22)
	ccFake.wsConnections.Store(33)

	eventuallyTrue(t, 3*time.Second, func() bool {
		return mr.PubSubNumSub(a.broadcastChannel())[a.broadcastChannel()] >= 3
	}, "all three nodes must be subscribed")

	// Extra settle time: heartbeats need to land so GetClusterInfos sees
	// the other two and sets expected peer count to 2.
	eventuallyTrue(t, 3*time.Second, func() bool {
		infos, err := a.GetClusterInfos()
		return err == nil && len(infos) == 3
	}, "all three nodes must have written their heartbeat keys")

	stats, appErr := a.GetClusterStats(nil)
	require.Nil(t, appErr)
	require.Len(t, stats, 3, "expected stats for self + 2 peers")

	// Map by node ID so we can assert without depending on response order.
	byID := map[string]int{}
	for _, s := range stats {
		byID[s.Id] = s.TotalWebsocketConnections
	}
	require.Equal(t, 11, byID["node-stats-a"])
	require.Equal(t, 22, byID["node-stats-b"])
	require.Equal(t, 33, byID["node-stats-c"])
}

func TestRedisWebConnCountAggregatesAcrossPeers(t *testing.T) {
	// Each node reports its OWN local count in its RPC reply; the caller
	// sums only peer responses (not self). This test verifies:
	//   * peers with zero count contribute zero (not "absent from map")
	//   * non-zero peer counts are summed correctly
	//   * the requester's own count is NOT included in the returned sum
	//     (upstream web_hub.go combines local + cluster separately)
	mr := miniredis.RunT(t)

	a, aFake := newTestCluster(t, mr.Addr(), "node-wcc-a")
	b, bFake := newTestCluster(t, mr.Addr(), "node-wcc-b")
	cc, ccFake := newTestCluster(t, mr.Addr(), "node-wcc-c")
	a.StartInterNodeCommunication()
	b.StartInterNodeCommunication()
	cc.StartInterNodeCommunication()
	defer a.StopInterNodeCommunication()
	defer b.StopInterNodeCommunication()
	defer cc.StopInterNodeCommunication()

	// a: 7, b: 3, c: 0 for user-123
	aFake.setUserConnCount("user-123", 7)
	bFake.setUserConnCount("user-123", 3)
	ccFake.setUserConnCount("user-123", 0)

	eventuallyTrue(t, 3*time.Second, func() bool {
		return mr.PubSubNumSub(a.broadcastChannel())[a.broadcastChannel()] >= 3
	}, "all three nodes must be subscribed")

	eventuallyTrue(t, 3*time.Second, func() bool {
		infos, err := a.GetClusterInfos()
		return err == nil && len(infos) == 3
	}, "all three nodes must have written their heartbeat keys")

	// Query from node a. Expect only b + c counts: 3 + 0 = 3.
	total, appErr := a.WebConnCountForUser("user-123")
	require.Nil(t, appErr)
	require.Equal(t, 3, total,
		"WebConnCountForUser should sum peer counts only (not self)")
}

func TestRedisWebConnCountZeroWithSingleNode(t *testing.T) {
	// A single-node cluster has no peers to query; WebConnCountForUser must
	// return 0 immediately, not hang waiting for phantom replies.
	mr := miniredis.RunT(t)

	a, aFake := newTestCluster(t, mr.Addr(), "node-solo")
	a.StartInterNodeCommunication()
	defer a.StopInterNodeCommunication()

	aFake.setUserConnCount("user-x", 5)

	eventuallyTrue(t, 2*time.Second, func() bool {
		infos, err := a.GetClusterInfos()
		return err == nil && len(infos) == 1
	}, "self-heartbeat must be present")

	start := time.Now()
	total, appErr := a.WebConnCountForUser("user-x")
	elapsed := time.Since(start)

	require.Nil(t, appErr)
	require.Equal(t, 0, total, "no peers = zero aggregate, regardless of local count")
	require.Less(t, elapsed, 500*time.Millisecond,
		"single-node path must not wait for the RPC timeout")
}

func TestRedisGetPluginStatusesAggregates(t *testing.T) {
	// Peers with disjoint plugin sets; GetPluginStatuses should concatenate
	// their results.
	mr := miniredis.RunT(t)

	a, _ := newTestCluster(t, mr.Addr(), "node-ps-a")
	b, bFake := newTestCluster(t, mr.Addr(), "node-ps-b")
	cc, ccFake := newTestCluster(t, mr.Addr(), "node-ps-c")
	a.StartInterNodeCommunication()
	b.StartInterNodeCommunication()
	cc.StartInterNodeCommunication()
	defer a.StopInterNodeCommunication()
	defer b.StopInterNodeCommunication()
	defer cc.StopInterNodeCommunication()

	// a has one plugin; b has two; c has one. Aggregator on a must return
	// b's two + c's one = 3 (self excluded, matching caller semantics in
	// plugin_statuses.go which merges its own statuses separately).
	bFake.setPluginStatuses(model.PluginStatuses{
		{PluginId: "plugin-on-b-1", State: model.PluginStateRunning},
		{PluginId: "plugin-on-b-2", State: model.PluginStateRunning},
	})
	ccFake.setPluginStatuses(model.PluginStatuses{
		{PluginId: "plugin-on-c", State: model.PluginStateFailedToStart},
	})

	eventuallyTrue(t, 3*time.Second, func() bool {
		infos, err := a.GetClusterInfos()
		return err == nil && len(infos) == 3
	}, "all three heartbeats must be visible")

	got, appErr := a.GetPluginStatuses()
	require.Nil(t, appErr)
	require.Len(t, got, 3, "expected 2 from b + 1 from c (self excluded)")

	ids := map[string]bool{}
	for _, s := range got {
		ids[s.PluginId] = true
	}
	require.True(t, ids["plugin-on-b-1"])
	require.True(t, ids["plugin-on-b-2"])
	require.True(t, ids["plugin-on-c"])
}

func TestRedisClusterNotifyMsgDropsLoopback(t *testing.T) {
	// NotifyMsg is an exported entry on ClusterInterface; ensure it
	// rejects envelopes that claim our own node as sender. This guards
	// against bugs where a peer (or mis-configured test) replays our
	// own traffic to us.
	mr := miniredis.RunT(t)
	c, _ := newTestCluster(t, mr.Addr(), "node-self")
	c.StartInterNodeCommunication()
	defer c.StopInterNodeCommunication()

	got := make(chan struct{}, 1)
	c.RegisterClusterMessageHandler("evt", func(*model.ClusterMessage) { got <- struct{}{} })

	// Build an envelope as if node-self had sent it.
	env := []byte(`{"sender":"node-self","ts":1,"msg":{"event":"evt"}}`)
	c.NotifyMsg(env)

	select {
	case <-got:
		t.Fatal("NotifyMsg failed to filter own-sender envelope")
	case <-time.After(100 * time.Millisecond):
	}
}
