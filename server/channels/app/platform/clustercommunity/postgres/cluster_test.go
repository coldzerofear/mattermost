// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package postgres

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/shared/mlog"
)

// Integration-tests are gated on MM_CLUSTER_PG_TEST_DSN. The DSN must point
// at a writable PostgreSQL instance; the tests will CREATE TABLE and issue
// LISTEN/NOTIFY. Set this in CI or locally before running:
//
//	export MM_CLUSTER_PG_TEST_DSN="host=/var/run/postgresql dbname=clustertest sslmode=disable"
//	go test -race ./channels/app/platform/clustercommunity/postgres/
//
// Without the variable, tests are skipped so regular unit-test runs don't
// depend on a live database.
const testDSNEnv = "MM_CLUSTER_PG_TEST_DSN"

func requirePGDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv(testDSNEnv)
	if dsn == "" {
		t.Skipf("%s not set; skipping postgres integration test", testDSNEnv)
	}
	return dsn
}

// fakePlatform satisfies the narrow platformDeps interface. It deliberately
// omits heartbeat / discovery hooks so the integration tests can drive the
// cluster without a full PlatformService.
type fakePlatform struct {
	logger             *mlog.Logger
	leaderChangedCount atomic.Int32
	reloadCount        atomic.Int32
	reloadErr          error
	wsConnections      atomic.Int64
	pluginStatuses     model.PluginStatuses
}

func newFakePlatform(t *testing.T) *fakePlatform {
	return &fakePlatform{logger: mlog.CreateConsoleTestLogger(t)}
}

func (f *fakePlatform) Log() mlog.LoggerIFace                { return f.logger }
func (f *fakePlatform) InvokeClusterLeaderChangedListeners() { f.leaderChangedCount.Add(1) }
func (f *fakePlatform) ReloadConfig() error {
	f.reloadCount.Add(1)
	return f.reloadErr
}
func (f *fakePlatform) TotalWebsocketConnections() int {
	return int(f.wsConnections.Load())
}

// WebConnCountForUser / GetPluginStatuses are exercised by the redis test
// suite; the postgres integration tests inherit the same bus/ plumbing and
// do not re-validate it (keeps the PG test runtime short). These stubs
// exist only to satisfy platformDeps so postgres.New compiles.
func (f *fakePlatform) WebConnCountForUser(userID string) int { return 0 }
func (f *fakePlatform) GetPluginStatuses() (model.PluginStatuses, *model.AppError) {
	return f.pluginStatuses, nil
}

// newTestCluster constructs a Cluster against the shared test DSN. Each test
// run uses a unique channel name derived from the test name so parallel
// tests do not cross-talk. The caller is responsible for StopInterNodeCommunication.
func newTestCluster(t *testing.T, nodeID string) (*Cluster, *fakePlatform) {
	t.Helper()
	dsn := requirePGDSN(t)

	fake := newFakePlatform(t)
	c, err := New(fake, &Options{
		NodeID:      nodeID,
		ClusterName: "test-cluster",
		DSN:         dsn,
		// One channel per test name keeps concurrent runs isolated.
		ChannelName: "mm_cluster_test_" + sanitize(t.Name()),
	})
	require.NoError(t, err)
	return c, fake
}

// sanitize turns a test name into a valid PostgreSQL identifier suffix.
// Channel names must be identifiers, so slashes and other punctuation from
// subtests like TestFoo/subcase need to be scrubbed.
func sanitize(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case ch >= 'a' && ch <= 'z', ch >= 'A' && ch <= 'Z', ch >= '0' && ch <= '9':
			out = append(out, ch)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}

func eventuallyTrue(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("condition never became true within %v: %s", timeout, msg)
}

func TestPGEnsureSchemaIdempotent(t *testing.T) {
	c, _ := newTestCluster(t, "node-schema")

	// New already calls ensureSchema once; a second call must be a no-op.
	require.NoError(t, c.ensureSchema(context.Background()))
	require.NoError(t, c.ensureSchema(context.Background()))

	// Table exists and is writeable.
	ctx := context.Background()
	require.NoError(t, c.insertAndNotify(ctx, model.NewId(), "", []byte(`{"x":1}`), nowMS()))

	_ = c.db.Close()
}

func TestPGSendReceiveBroadcast(t *testing.T) {
	sender, _ := newTestCluster(t, "node-sender")
	receiver, _ := newTestCluster(t, "node-receiver")
	// Both test clusters must LISTEN on the SAME channel to see each other's
	// notifications. newTestCluster derives channel from t.Name, so both get
	// the same value within one test.

	sender.StartInterNodeCommunication()
	receiver.StartInterNodeCommunication()
	defer sender.StopInterNodeCommunication()
	defer receiver.StopInterNodeCommunication()

	ch := make(chan *model.ClusterMessage, 1)
	receiver.RegisterClusterMessageHandler(model.ClusterEventPublish, func(msg *model.ClusterMessage) {
		ch <- msg
	})

	// Allow the listener a moment to finish setting up LISTEN on the server.
	// pq.Listener.Listen returns before the server has actually registered
	// the subscription, so publishing immediately can lose the first event.
	time.Sleep(200 * time.Millisecond)

	sender.SendClusterMessage(&model.ClusterMessage{
		Event: model.ClusterEventPublish,
		Data:  []byte("hello-pg"),
	})

	select {
	case got := <-ch:
		require.Equal(t, []byte("hello-pg"), got.Data)
	case <-time.After(5 * time.Second):
		t.Fatal("receiver did not get the broadcast within 5s")
	}
}

func TestPGLoopbackFiltered(t *testing.T) {
	c, _ := newTestCluster(t, "node-self-pg")
	c.StartInterNodeCommunication()
	defer c.StopInterNodeCommunication()

	delivered := make(chan struct{}, 1)
	c.RegisterClusterMessageHandler(model.ClusterEventPublish, func(*model.ClusterMessage) {
		delivered <- struct{}{}
	})

	time.Sleep(200 * time.Millisecond)
	c.SendClusterMessage(&model.ClusterMessage{Event: model.ClusterEventPublish, Data: []byte("x")})

	select {
	case <-delivered:
		t.Fatal("sender received its own broadcast (envelope.Sender filter broken)")
	case <-time.After(500 * time.Millisecond):
	}
}

func TestPGDirectMessageFilter(t *testing.T) {
	// Postgres does direct-message filtering at the SQL layer in
	// fetchPayload: rows whose target is neither '' nor self are skipped.
	// This test drives that path explicitly.
	a, _ := newTestCluster(t, "pg-a")
	b, _ := newTestCluster(t, "pg-b")
	cc, _ := newTestCluster(t, "pg-c")
	a.StartInterNodeCommunication()
	b.StartInterNodeCommunication()
	cc.StartInterNodeCommunication()
	defer a.StopInterNodeCommunication()
	defer b.StopInterNodeCommunication()
	defer cc.StopInterNodeCommunication()

	bGot := make(chan struct{}, 1)
	cGot := make(chan struct{}, 1)
	b.RegisterClusterMessageHandler("direct", func(*model.ClusterMessage) { bGot <- struct{}{} })
	cc.RegisterClusterMessageHandler("direct", func(*model.ClusterMessage) { cGot <- struct{}{} })

	time.Sleep(200 * time.Millisecond)
	require.NoError(t, a.SendClusterMessageToNode("pg-b", &model.ClusterMessage{Event: "direct"}))

	select {
	case <-bGot:
	case <-time.After(3 * time.Second):
		t.Fatal("target did not receive the direct message")
	}

	select {
	case <-cGot:
		t.Fatal("non-target received a direct message")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestPGFetchPayloadFilters(t *testing.T) {
	// Unit-level: fetchPayload must return sql.ErrNoRows when target is set
	// to a different node. Exercised directly to lock in the WHERE clause.
	c, _ := newTestCluster(t, "pg-fetch")
	defer c.db.Close()

	ctx := context.Background()
	ownID := model.NewId()
	peerID := model.NewId()
	broadcastID := model.NewId()

	require.NoError(t, c.insertAndNotify(ctx, ownID, c.node.ID, []byte("own"), nowMS()))
	require.NoError(t, c.insertAndNotify(ctx, peerID, "some-other-node", []byte("peer"), nowMS()))
	require.NoError(t, c.insertAndNotify(ctx, broadcastID, "", []byte("all"), nowMS()))

	// Own target: visible.
	p, err := c.fetchPayload(ctx, ownID)
	require.NoError(t, err)
	require.Equal(t, []byte("own"), p)

	// Broadcast: visible.
	p, err = c.fetchPayload(ctx, broadcastID)
	require.NoError(t, err)
	require.Equal(t, []byte("all"), p)

	// Directed at other node: invisible to us.
	_, err = c.fetchPayload(ctx, peerID)
	require.Error(t, err, "fetchPayload must filter out messages targeted at other nodes")

	// Cleanup — otherwise rows accumulate across test runs until gc.
	_, _ = c.db.ExecContext(ctx,
		`DELETE FROM cluster_messages WHERE id IN ($1, $2, $3)`,
		ownID, peerID, broadcastID)
}

func TestPGGcDeletesExpired(t *testing.T) {
	c, _ := newTestCluster(t, "pg-gc")
	defer c.db.Close()

	ctx := context.Background()
	oldID := model.NewId()
	freshID := model.NewId()

	// Old row: 10 minutes in the past. Fresh row: just now.
	require.NoError(t, c.insertAndNotify(ctx, oldID, "", []byte("old"), nowMS()-10*60_000))
	require.NoError(t, c.insertAndNotify(ctx, freshID, "", []byte("fresh"), nowMS()))

	c.gcOldMessages(ctx, int64(messageRetention/time.Millisecond))

	_, err := c.fetchPayload(ctx, oldID)
	require.Error(t, err, "old row should have been deleted by gc")

	p, err := c.fetchPayload(ctx, freshID)
	require.NoError(t, err)
	require.Equal(t, []byte("fresh"), p)

	_, _ = c.db.ExecContext(ctx, `DELETE FROM cluster_messages WHERE id = $1`, freshID)
}

func TestPGAdvisoryLockLeaderElection(t *testing.T) {
	// With three contenders hitting the same PG, pg_try_advisory_lock must
	// grant the lock to exactly one. When that winner releases, another
	// contender must pick it up.
	a, _ := newTestCluster(t, "pg-leader-a")
	b, _ := newTestCluster(t, "pg-leader-b")
	cc, _ := newTestCluster(t, "pg-leader-c")

	a.StartInterNodeCommunication()
	b.StartInterNodeCommunication()
	cc.StartInterNodeCommunication()

	// Give the leaderLoop one tick to run.
	eventuallyTrue(t, 10*time.Second, func() bool {
		leaders := 0
		if a.IsLeader() {
			leaders++
		}
		if b.IsLeader() {
			leaders++
		}
		if cc.IsLeader() {
			leaders++
		}
		return leaders == 1
	}, "expected exactly one advisory-lock leader")

	// Identify the winner and stop it. Either of the other two can pick up
	// the lock — which one wins the race is not something this test wants
	// to pin down (the order of leaderLoop ticks across processes is not
	// deterministic). What we require is "some remaining node becomes
	// leader in bounded time".
	nodes := []*Cluster{a, b, cc}
	var winner *Cluster
	for _, n := range nodes {
		if n.IsLeader() {
			winner = n
			break
		}
	}
	require.NotNil(t, winner)

	winner.StopInterNodeCommunication()
	// Ensure all three get stopped eventually so the test cleanup is clean.
	for _, n := range nodes {
		if n != winner {
			defer n.StopInterNodeCommunication()
		}
	}

	eventuallyTrue(t, 15*time.Second, func() bool {
		leaders := 0
		for _, n := range nodes {
			if n != winner && n.IsLeader() {
				leaders++
			}
		}
		return leaders == 1
	}, "exactly one of the two survivors should pick up the advisory lock")
}

func TestPGInsertPayloadIsBinarySafe(t *testing.T) {
	// The payload column is BYTEA; verify we can round-trip non-UTF-8 bytes
	// that would be rejected by a TEXT column. This guards against a future
	// schema change that mistakenly uses TEXT.
	c, _ := newTestCluster(t, "pg-binary")
	defer c.db.Close()

	ctx := context.Background()
	id := model.NewId()
	binary := []byte{0x00, 0xff, 0xfe, 0x01, 0x00, 0xc3, 0x28}

	require.NoError(t, c.insertAndNotify(ctx, id, "", binary, nowMS()))

	got, err := c.fetchPayload(ctx, id)
	require.NoError(t, err)
	require.Equal(t, binary, got)

	_, _ = c.db.ExecContext(ctx, `DELETE FROM cluster_messages WHERE id = $1`, id)
}

// assertChannelName is here mostly so go vet sees sanitize is used when some
// other test file is compiled out. Left as a trivial runtime check.
func assertChannelName(t *testing.T, s string) {
	t.Helper()
	require.NotContains(t, s, " ")
	require.NotContains(t, s, "/")
}

func TestSanitize(t *testing.T) {
	assertChannelName(t, sanitize("TestFoo/Bar Baz-123"))
	require.Equal(t, "abc_123", sanitize("abc/123"))
	require.Equal(t, "_", sanitize(fmt.Sprintf("%c", '\x00')))
}
