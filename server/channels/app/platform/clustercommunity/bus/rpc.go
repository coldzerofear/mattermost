// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package bus

import (
	"fmt"
	"sync"
	"time"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/shared/mlog"
)

// RPC props keys placed on ClusterMessage.Props for request/response pairing.
// Using a dedicated prefix keeps them out of any Props namespace a handler
// might use for its own business data.
const (
	rpcPropsID        = "rpc_id"
	rpcPropsRequester = "rpc_requester"
)

// Sender is the surface RPC needs from a backend (redis.Cluster /
// postgres.Cluster). It is satisfied implicitly by each backend.
type Sender interface {
	NodeID() string
	SendClusterMessage(msg *model.ClusterMessage)
	SendClusterMessageToNode(nodeID string, msg *model.ClusterMessage) error
}

// RPC implements a broadcast-request / directed-response pattern over the
// cluster message bus. It is used for admin queries (GetClusterStats,
// GetLogs, ...) where a node needs aggregated data from all peers.
//
// Lifecycle:
//
//	r := NewRPC(sender, registry, logger)
//	r.RegisterResponder(req, resp, buildReply)   // once, at startup
//	r.RegisterAwaiter(resp)                      // once, at startup
//	replies := r.Broadcast(req, payload, 5, 2*time.Second)
//
// Concurrency: Broadcast is safe to call from any goroutine; each call
// generates its own correlation ID and private response channel. The
// pending map is protected by mu.
type RPC struct {
	sender   Sender
	handlers *HandlerRegistry
	logger   mlog.LoggerIFace

	mu      sync.Mutex
	pending map[string]chan *model.ClusterMessage
}

func NewRPC(sender Sender, handlers *HandlerRegistry, logger mlog.LoggerIFace) *RPC {
	return &RPC{
		sender:   sender,
		handlers: handlers,
		logger:   logger,
		pending:  make(map[string]chan *model.ClusterMessage),
	}
}

// RegisterResponder installs a handler for reqEvent that builds a reply via
// buildReply and sends it back to the requester using respEvent. A nil
// buildReply means "ignore the request silently" — useful when a node opts
// out of participating in a given RPC without breaking requesters.
//
// Register must be called once at startup, before StartInterNodeCommunication
// so the handler is in place before messages begin flowing.
func (r *RPC) RegisterResponder(
	reqEvent, respEvent model.ClusterEvent,
	buildReply func(request *model.ClusterMessage) []byte,
) {
	r.handlers.RegisterClusterMessageHandler(reqEvent, func(req *model.ClusterMessage) {
		id, ok := req.Props[rpcPropsID]
		requester, hasReq := req.Props[rpcPropsRequester]
		if !ok || !hasReq {
			// Malformed RPC request: missing correlation metadata. Skip
			// rather than reply to nobody.
			r.logger.Warn("RPC request missing id or requester", mlog.String("event", string(reqEvent)))
			return
		}
		if requester == r.sender.NodeID() {
			// Never reply to our own broadcast — the pending map lookup on
			// our side would still resolve, but the semantics of "cluster
			// peers" exclude self.
			return
		}
		if buildReply == nil {
			return
		}

		payload := buildReply(req)
		resp := &model.ClusterMessage{
			Event:    respEvent,
			SendType: model.ClusterSendBestEffort,
			Props:    map[string]string{rpcPropsID: id},
			Data:     payload,
		}
		if err := r.sender.SendClusterMessageToNode(requester, resp); err != nil {
			r.logger.Warn("RPC response send failed",
				mlog.String("event", string(respEvent)),
				mlog.String("requester", requester),
				mlog.Err(err))
		}
	})
}

// RegisterAwaiter installs a response handler for respEvent. Incoming responses
// are demultiplexed by rpcPropsID onto the channel registered by Broadcast.
// Responses without a known ID are dropped (request may have timed out).
func (r *RPC) RegisterAwaiter(respEvent model.ClusterEvent) {
	r.handlers.RegisterClusterMessageHandler(respEvent, func(resp *model.ClusterMessage) {
		id, ok := resp.Props[rpcPropsID]
		if !ok {
			return
		}
		r.mu.Lock()
		ch := r.pending[id]
		r.mu.Unlock()
		if ch == nil {
			// Request already timed out / cleaned up; benign late arrival.
			return
		}
		// Non-blocking send: if the waiter has exited the select already
		// because of timeout, dropping is correct.
		select {
		case ch <- resp:
		default:
		}
	})
}

// Broadcast sends reqEvent to all peers and collects up to maxResponses
// within timeout. Returns whatever arrived by the deadline; callers that
// need a hard "we heard from N nodes" guarantee should compare len(result)
// to their expectation.
//
// The bufferSize equals maxResponses so responders never block on a full
// channel even if all peers answer at the same instant.
func (r *RPC) Broadcast(
	reqEvent model.ClusterEvent,
	payload []byte,
	maxResponses int,
	timeout time.Duration,
) []*model.ClusterMessage {
	if maxResponses <= 0 {
		return nil
	}

	id := model.NewId()
	ch := make(chan *model.ClusterMessage, maxResponses)

	r.mu.Lock()
	r.pending[id] = ch
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		delete(r.pending, id)
		r.mu.Unlock()
	}()

	req := &model.ClusterMessage{
		Event:    reqEvent,
		SendType: model.ClusterSendBestEffort,
		Props: map[string]string{
			rpcPropsID:        id,
			rpcPropsRequester: r.sender.NodeID(),
		},
		Data: payload,
	}
	r.sender.SendClusterMessage(req)

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	out := make([]*model.ClusterMessage, 0, maxResponses)
	for len(out) < maxResponses {
		select {
		case msg := <-ch:
			out = append(out, msg)
		case <-deadline.C:
			return out
		}
	}
	return out
}

// ErrRPCPending is returned in scenarios where the RPC helper wants to signal
// "request accepted but not yet answered" without pulling in other error
// types. Currently only used by GetClusterStats; re-exported here so every
// caller shares one constant.
var ErrRPCPending = fmt.Errorf("rpc: pending response")
