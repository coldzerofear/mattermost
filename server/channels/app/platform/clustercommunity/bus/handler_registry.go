// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package bus

import (
	"sync"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/v8/einterfaces"
)

// HandlerRegistry is a thread-safe map of ClusterEvent -> handler.
// It exists as a standalone struct so that both the Redis and PostgreSQL
// backends can embed it and share RegisterClusterMessageHandler / Dispatch
// logic instead of duplicating identical code.
//
// Registration is expected to happen once, during server startup, before
// StartInterNodeCommunication fires. Dispatch is called from the message
// reader goroutine on every incoming cluster message.
type HandlerRegistry struct {
	mu       sync.RWMutex
	handlers map[model.ClusterEvent]einterfaces.ClusterMessageHandler
}

func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{
		handlers: map[model.ClusterEvent]einterfaces.ClusterMessageHandler{},
	}
}

// RegisterClusterMessageHandler registers a handler for the given event.
// If a handler for the event already exists, it is overwritten (matches the
// upstream enterprise behavior).
func (r *HandlerRegistry) RegisterClusterMessageHandler(event model.ClusterEvent, h einterfaces.ClusterMessageHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[event] = h
}

// Dispatch invokes the handler registered for msg.Event, if any. Unknown
// events are silently dropped; this matches enterprise behavior where
// newer nodes may publish events that older nodes don't recognize.
func (r *HandlerRegistry) Dispatch(msg *model.ClusterMessage) {
	r.mu.RLock()
	h := r.handlers[msg.Event]
	r.mu.RUnlock()
	if h != nil {
		h(msg)
	}
}
