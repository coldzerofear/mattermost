// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package bus

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/stretchr/testify/require"
)

func TestHandlerRegistryDispatch(t *testing.T) {
	r := NewHandlerRegistry()
	var got atomic.Pointer[model.ClusterMessage]

	r.RegisterClusterMessageHandler(model.ClusterEventPublish, func(msg *model.ClusterMessage) {
		got.Store(msg)
	})

	msg := &model.ClusterMessage{Event: model.ClusterEventPublish, Data: []byte("x")}
	r.Dispatch(msg)

	require.Equal(t, msg, got.Load())
}

func TestHandlerRegistryUnknownEventSilent(t *testing.T) {
	// Dispatching an event with no registered handler must not panic. This
	// matches upstream enterprise behavior where a newer peer can publish
	// events unknown to an older node.
	r := NewHandlerRegistry()
	r.Dispatch(&model.ClusterMessage{Event: "never_registered"})
	// No assertion beyond "did not panic".
}

func TestHandlerRegistryOverwrite(t *testing.T) {
	// Second Register for the same event overwrites the first. We pin
	// this because upstream's map semantics imply it and other code may
	// rely on "register last wins".
	r := NewHandlerRegistry()
	var firstCalls, secondCalls atomic.Int32

	r.RegisterClusterMessageHandler("evt", func(*model.ClusterMessage) { firstCalls.Add(1) })
	r.RegisterClusterMessageHandler("evt", func(*model.ClusterMessage) { secondCalls.Add(1) })

	r.Dispatch(&model.ClusterMessage{Event: "evt"})
	require.Equal(t, int32(0), firstCalls.Load())
	require.Equal(t, int32(1), secondCalls.Load())
}

func TestHandlerRegistryConcurrent(t *testing.T) {
	// The registry must be safe for concurrent Register and Dispatch.
	// Registration happens at startup in production but tests / hot-reload
	// scenarios can overlap.
	r := NewHandlerRegistry()
	var counter atomic.Int64

	const events = 10
	const dispatchers = 8
	const iterations = 500

	var wg sync.WaitGroup

	// Producer: registers handlers on random events.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			ev := model.ClusterEvent("evt_" + string(rune('a'+(i%events))))
			r.RegisterClusterMessageHandler(ev, func(*model.ClusterMessage) {
				counter.Add(1)
			})
		}
	}()

	// Consumers: dispatch events concurrently.
	for d := 0; d < dispatchers; d++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				ev := model.ClusterEvent("evt_" + string(rune('a'+(i%events))))
				r.Dispatch(&model.ClusterMessage{Event: ev})
			}
		}()
	}

	wg.Wait()
	// No assertion on exact count — the test is for race detection via
	// `go test -race`. Counter.Load() just prevents the handler being
	// dead code.
	_ = counter.Load()
}
