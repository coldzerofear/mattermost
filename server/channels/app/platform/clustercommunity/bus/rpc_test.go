// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package bus

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/shared/mlog"
)

// inMemoryBus is a test-local implementation of Sender that delivers
// messages directly to connected handlers without touching any network.
// It lets us exercise RPC semantics (correlation ID, timeout, pending
// cleanup) without standing up miniredis or Postgres.
type inMemoryBus struct {
	mu    sync.Mutex
	nodes map[string]*HandlerRegistry
}

func newInMemoryBus() *inMemoryBus {
	return &inMemoryBus{nodes: make(map[string]*HandlerRegistry)}
}

func (b *inMemoryBus) addNode(id string, h *HandlerRegistry) {
	b.mu.Lock()
	b.nodes[id] = h
	b.mu.Unlock()
}

// busSender connects a node into the inMemoryBus. It implements bus.Sender
// and delivers messages synchronously (but in a goroutine to avoid
// self-dispatch during a handler that re-enters).
type busSender struct {
	bus    *inMemoryBus
	nodeID string
}

func (s *busSender) NodeID() string { return s.nodeID }

func (s *busSender) SendClusterMessage(msg *model.ClusterMessage) {
	s.bus.mu.Lock()
	targets := make(map[string]*HandlerRegistry, len(s.bus.nodes))
	for id, h := range s.bus.nodes {
		if id == s.nodeID {
			continue // loopback filter
		}
		targets[id] = h
	}
	s.bus.mu.Unlock()
	for _, h := range targets {
		go h.Dispatch(msg)
	}
}

func (s *busSender) SendClusterMessageToNode(nodeID string, msg *model.ClusterMessage) error {
	s.bus.mu.Lock()
	h := s.bus.nodes[nodeID]
	s.bus.mu.Unlock()
	if h == nil {
		return nil // unknown node; match enterprise semantics which also drop silently
	}
	go h.Dispatch(msg)
	return nil
}

// eventuallyTrue polls cond every 10ms and fails if timeout elapses.
func eventuallyTrue(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition never satisfied within %v: %s", timeout, msg)
}

func logger(t *testing.T) mlog.LoggerIFace {
	return mlog.CreateConsoleTestLogger(t)
}

func TestRPCBroadcastCollectsAllResponders(t *testing.T) {
	b := newInMemoryBus()

	const requesterID = "n-req"
	reqHandlers := NewHandlerRegistry()
	b.addNode(requesterID, reqHandlers)

	const (
		reqEvt  model.ClusterEvent = "test_req"
		respEvt model.ClusterEvent = "test_resp"
	)

	// Three peers responding with unique payloads.
	for _, id := range []string{"n-a", "n-b", "n-c"} {
		h := NewHandlerRegistry()
		b.addNode(id, h)
		rpcPeer := NewRPC(&busSender{bus: b, nodeID: id}, h, logger(t))
		peerID := id // capture
		rpcPeer.RegisterResponder(reqEvt, respEvt, func(*model.ClusterMessage) []byte {
			return []byte("from-" + peerID)
		})
	}

	requesterRPC := NewRPC(&busSender{bus: b, nodeID: requesterID}, reqHandlers, logger(t))
	requesterRPC.RegisterAwaiter(respEvt)

	replies := requesterRPC.Broadcast(reqEvt, nil, 3, 2*time.Second)
	require.Len(t, replies, 3, "expected one reply from each peer")

	gotBodies := make(map[string]bool)
	for _, r := range replies {
		gotBodies[string(r.Data)] = true
	}
	require.True(t, gotBodies["from-n-a"])
	require.True(t, gotBodies["from-n-b"])
	require.True(t, gotBodies["from-n-c"])
}

func TestRPCBroadcastTimesOutWithFewerPeers(t *testing.T) {
	// If fewer peers answer than requested, Broadcast must return what it has
	// after the timeout — it must not block forever.
	b := newInMemoryBus()

	const requesterID = "n-req"
	reqHandlers := NewHandlerRegistry()
	b.addNode(requesterID, reqHandlers)

	const (
		reqEvt  model.ClusterEvent = "slow_req"
		respEvt model.ClusterEvent = "slow_resp"
	)

	// Only one responder; ask for 5 within 200ms.
	h := NewHandlerRegistry()
	b.addNode("one-peer", h)
	peerRPC := NewRPC(&busSender{bus: b, nodeID: "one-peer"}, h, logger(t))
	peerRPC.RegisterResponder(reqEvt, respEvt, func(*model.ClusterMessage) []byte { return []byte("x") })

	requesterRPC := NewRPC(&busSender{bus: b, nodeID: requesterID}, reqHandlers, logger(t))
	requesterRPC.RegisterAwaiter(respEvt)

	start := time.Now()
	replies := requesterRPC.Broadcast(reqEvt, nil, 5, 200*time.Millisecond)
	elapsed := time.Since(start)

	require.Len(t, replies, 1, "should have received exactly one reply before timeout")
	require.Greater(t, elapsed, 150*time.Millisecond, "Broadcast should have waited roughly until timeout")
	require.Less(t, elapsed, 500*time.Millisecond, "Broadcast should not have waited much past timeout")
}

func TestRPCResponderIgnoresLoopback(t *testing.T) {
	// When a node receives its own broadcast, RegisterResponder's handler
	// must not build a reply. Testing this prevents an infinite response
	// loop where each self-reply re-triggers the responder.
	b := newInMemoryBus()

	const id = "n-solo"
	h := NewHandlerRegistry()
	b.addNode(id, h)

	const (
		reqEvt  model.ClusterEvent = "self_req"
		respEvt model.ClusterEvent = "self_resp"
	)

	var buildCalls atomic.Int32
	rpc := NewRPC(&busSender{bus: b, nodeID: id}, h, logger(t))
	rpc.RegisterResponder(reqEvt, respEvt, func(*model.ClusterMessage) []byte {
		buildCalls.Add(1)
		return nil
	})

	// Inject a request that claims to come from ourselves.
	h.Dispatch(&model.ClusterMessage{
		Event: reqEvt,
		Props: map[string]string{
			rpcPropsID:        "fake",
			rpcPropsRequester: id, // self
		},
	})

	// Give it a moment; buildReply must not have been invoked.
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, int32(0), buildCalls.Load(), "responder built a reply to its own broadcast")
}

func TestRPCLateResponseDropped(t *testing.T) {
	// Responses with an unknown rpc_id (e.g. arriving after the caller
	// timed out) must be silently dropped — not crash the awaiter handler.
	b := newInMemoryBus()
	h := NewHandlerRegistry()
	b.addNode("n", h)

	rpc := NewRPC(&busSender{bus: b, nodeID: "n"}, h, logger(t))
	rpc.RegisterAwaiter("resp_evt")

	h.Dispatch(&model.ClusterMessage{
		Event: "resp_evt",
		Props: map[string]string{rpcPropsID: "no-such-id"},
		Data:  []byte("late"),
	})
	// Just ensuring no panic.
}
