// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/lib/pq"

	"github.com/mattermost/mattermost/server/public/shared/mlog"
	"github.com/mattermost/mattermost/server/v8/channels/app/platform/clustercommunity/bus"
)

// Listener reconnect tuning. pq.Listener handles backoff internally between
// these bounds and resets the delay after each successful connect.
const (
	listenerMinReconnect = 2 * time.Second
	listenerMaxReconnect = 30 * time.Second

	// listenerPingInterval proactively round-trips the listener's connection.
	// LISTEN is idle by design — if a middlebox (K8s kube-proxy, load balancer
	// TCP idle timeout, cloud NAT gateway) drops the connection silently, the
	// Notify channel simply never delivers. Ping forces a command so we learn
	// about dead connections in bounded time.
	listenerPingInterval = 90 * time.Second

	// fetchTimeout bounds how long we'll wait to SELECT a payload before
	// giving up on a single notification. If the DB is that slow, the
	// backpressure needs to be on the caller's side.
	fetchTimeout = 3 * time.Second
)

// startListener opens a dedicated pq.Listener against the cluster DSN and
// subscribes to the configured channel. Returns an error if the initial
// LISTEN fails; the caller aborts Start in that case to surface the
// configuration problem loudly.
//
// pq.Listener internally runs a reconnection goroutine — we do not need to
// manage TCP lifecycle. We only drive the Notify channel from our listenerLoop.
func (c *Cluster) startListener() error {
	l := pq.NewListener(c.dsn, listenerMinReconnect, listenerMaxReconnect, c.onListenerEvent)
	if err := l.Listen(c.opts.ChannelName); err != nil {
		l.Close()
		return err
	}
	c.listener = l
	return nil
}

// onListenerEvent is called from inside pq.Listener's worker goroutine on
// every connection state transition. We only log; pq.Listener re-sends
// existing LISTENs automatically on reconnect, so there is no recovery work
// for us to do.
func (c *Cluster) onListenerEvent(ev pq.ListenerEventType, err error) {
	switch ev {
	case pq.ListenerEventConnected:
		c.logger.Info("Postgres cluster listener connected",
			mlog.String("channel", c.opts.ChannelName))
	case pq.ListenerEventDisconnected:
		c.health.RecordFailure()
		c.logger.Warn("Postgres cluster listener disconnected", mlog.Err(err))
	case pq.ListenerEventReconnected:
		c.logger.Info("Postgres cluster listener reconnected")
	case pq.ListenerEventConnectionAttemptFailed:
		c.health.RecordFailure()
		c.logger.Warn("Postgres cluster listener reconnect attempt failed", mlog.Err(err))
	}
}

// listenerLoop pulls notifications from pq.Listener.Notify and hands each one
// off to handleNotify. pq buffers 32 notifications; if we fall behind past
// that, the driver drops. We keep handleNotify fast (one SELECT + dispatch)
// to avoid that cliff.
//
// Ping on listenerPingInterval is our liveness check against silent TCP drops.
func (c *Cluster) listenerLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(listenerPingInterval)
	defer ticker.Stop()

	for {
		select {
		case n, ok := <-c.listener.Notify:
			if !ok {
				// Listener closed; normal on shutdown.
				return
			}
			if n == nil {
				// pq.Listener sends nil after a reconnect — purely a
				// signal to the caller that the stream might have gaps.
				// Community edition accepts at-most-once, so we ignore.
				continue
			}
			c.handleNotify(n.Extra)

		case <-ticker.C:
			if err := c.listener.Ping(); err != nil {
				c.health.RecordFailure()
				c.logger.Warn("Postgres cluster listener ping failed", mlog.Err(err))
			}

		case <-c.cancelCtx.Done():
			return
		}
	}
}

// handleNotify is the receive-side entry point: given the message ID in the
// notification's Extra field, fetch the envelope bytes, decode, and dispatch
// to the handler registered for the event type.
//
// All error paths are logged at warn/debug and return silently — a malformed
// or already-GC'd message must not tear down the loop.
func (c *Cluster) handleNotify(id string) {
	if id == "" {
		return
	}

	ctx, cancel := context.WithTimeout(c.cancelCtx, fetchTimeout)
	defer cancel()

	payload, err := c.fetchPayload(ctx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Three benign cases converge here:
			//   1. Broadcast was targeted at another node (filtered in SQL).
			//   2. GC ran before we got to SELECT.
			//   3. The sender rolled back the tx after NOTIFY was queued.
			// All silent at debug level.
			return
		}
		c.health.RecordFailure()
		c.logger.Warn("Failed to fetch cluster message payload",
			mlog.String("message_id", id),
			mlog.Err(err))
		return
	}

	env, err := bus.Decode(payload)
	if err != nil {
		c.logger.Warn("Failed to decode cluster envelope",
			mlog.String("message_id", id),
			mlog.Err(err))
		return
	}
	if env.Sender == c.node.ID {
		// Loopback: we wrote this row ourselves. SQL filter catches directed
		// messages; broadcasts to ourselves still reach us via NOTIFY.
		return
	}
	if env.Message == nil {
		return
	}

	c.health.RecordSuccess()
	c.Dispatch(env.Message)
}
