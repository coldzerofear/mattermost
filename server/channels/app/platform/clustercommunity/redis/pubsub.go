// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/rueidis"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/shared/mlog"
	"github.com/mattermost/mattermost/server/v8/channels/app/platform/clustercommunity/bus"
)

// Pub/Sub channel names are derived from opts.KeyPrefix so multiple Mattermost
// clusters can coexist on the same Redis instance without collisions.

func (c *Cluster) broadcastChannel() string {
	return c.opts.KeyPrefix + ":broadcast"
}

func (c *Cluster) nodeChannel(nodeID string) string {
	return c.opts.KeyPrefix + ":node:" + nodeID
}

// SendClusterMessage broadcasts to every other node. Failures are logged but
// not retried: WebSocket events carry "at-most-once" semantics in the upstream
// design, and the database is the source of truth for durable state. Adding
// retries here would risk duplicate delivery under network flap.
func (c *Cluster) SendClusterMessage(msg *model.ClusterMessage) {
	if err := c.publish(c.broadcastChannel(), "", msg); err != nil {
		c.health.RecordFailure()
		c.logger.Error("Failed to publish cluster message",
			mlog.String("event", string(msg.Event)),
			mlog.Err(err))
		return
	}
	c.health.RecordSuccess()
}

// SendClusterMessageToNode sends directly to a single target node.
// Returns an error (unlike SendClusterMessage) so the upstream plugin
// cluster-event contract can surface delivery failures to the caller.
func (c *Cluster) SendClusterMessageToNode(nodeID string, msg *model.ClusterMessage) error {
	if nodeID == "" {
		return fmt.Errorf("redis cluster: empty target node ID")
	}
	if err := c.publish(c.nodeChannel(nodeID), nodeID, msg); err != nil {
		c.health.RecordFailure()
		return err
	}
	c.health.RecordSuccess()
	return nil
}

func (c *Cluster) publish(channel, target string, msg *model.ClusterMessage) error {
	payload, err := bus.Encode(c.node.ID, target, msg)
	if err != nil {
		return fmt.Errorf("encode envelope: %w", err)
	}

	// 2s publish timeout: PUBLISH returns as soon as the server has accepted
	// the command; anything slower indicates a networking problem and we'd
	// rather fail the send than block the caller's goroutine.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cmd := c.cmdClient.B().Publish().
		Channel(channel).
		Message(rueidis.BinaryString(payload)).
		Build()

	return c.cmdClient.Do(ctx, cmd).Error()
}

// NotifyMsg is part of ClusterInterface. It is invoked internally by the
// subscribe loop (see onMessage) but is also exposed on the interface for
// symmetry with upstream. External callers can feed raw bytes into the
// dispatch pipeline if needed.
func (c *Cluster) NotifyMsg(buf []byte) {
	env, err := bus.Decode(buf)
	if err != nil {
		c.logger.Warn("Failed to decode cluster envelope", mlog.Err(err))
		return
	}
	if env.Sender == c.node.ID {
		// Loopback: we're subscribed to broadcast, so we also receive our own
		// publishes. Drop silently.
		return
	}
	if env.Target != "" && env.Target != c.node.ID {
		// Direct message meant for someone else; should not happen under
		// normal routing but guard against operator-driven manual publishes.
		return
	}
	if env.Message == nil {
		return
	}
	c.Dispatch(env.Message)
}

// subscribeLoop holds rueidis.Receive for the broadcast + node-direct channels.
// Receive blocks until the subscription ends (error or context cancel). On
// disruption we log and reconnect after a short backoff; rueidis reopens the
// TCP connection internally on the next Receive call.
func (c *Cluster) subscribeLoop() {
	defer c.wg.Done()

	broadcast := c.broadcastChannel()
	direct := c.nodeChannel(c.node.ID)

	for {
		if c.cancelCtx.Err() != nil {
			return
		}

		sub := c.subClient.B().Subscribe().Channel(broadcast, direct).Build()
		err := c.subClient.Receive(c.cancelCtx, sub, func(m rueidis.PubSubMessage) {
			c.NotifyMsg([]byte(m.Message))
		})

		if c.cancelCtx.Err() != nil {
			return
		}
		if err != nil {
			c.health.RecordFailure()
			c.logger.Error("Redis cluster subscribe interrupted, reconnecting",
				mlog.Err(err))
			// Small backoff before reconnect; prevents a hot loop when Redis
			// is down. 2s is long enough to let transient blips recover
			// without noticeably delaying real reconnects.
			select {
			case <-time.After(2 * time.Second):
			case <-c.cancelCtx.Done():
				return
			}
		}
	}
}
