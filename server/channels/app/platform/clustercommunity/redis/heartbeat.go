// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package redis

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/shared/mlog"
)

// Heartbeat design:
//
//   Each node writes its own key under <prefix>:nodes:<id> with a TTL slightly
//   longer than HeartbeatInterval * 3. If the node crashes, the key expires
//   naturally and peers stop considering it alive.
//
//   The value is a JSON-encoded model.ClusterInfo so GetClusterInfos can
//   return rich data without a second round trip.
//
// Leader election:
//
//   No distributed lock. Every node periodically SCANs <prefix>:nodes:* and
//   declares itself leader iff its own ID is the lexicographically smallest
//   live node. This is eventually-consistent: during churn, two nodes can
//   briefly both believe they are leader, so only assign to IsLeader logic
//   that tolerates brief duplication. (Mattermost uses IsLeader for scheduler
//   gating; the scheduler itself has its own idempotency via DB-level
//   uniqueness, so brief duplicate-leader is benign.)

func (c *Cluster) heartbeatKey() string {
	return c.opts.KeyPrefix + ":nodes:" + c.node.ID
}

func (c *Cluster) nodesKeyPattern() string {
	return c.opts.KeyPrefix + ":nodes:*"
}

func (c *Cluster) writeHeartbeat(ctx context.Context) error {
	info := c.GetMyClusterInfo()
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}

	ttl := c.opts.HeartbeatTTL
	if ttl <= 0 {
		ttl = 30 * time.Second
	}

	// SET key value EX ttl
	cmd := c.cmdClient.B().Set().
		Key(c.heartbeatKey()).
		Value(string(data)).
		ExSeconds(int64(ttl.Seconds())).
		Build()

	ctx2, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return c.cmdClient.Do(ctx2, cmd).Error()
}

func (c *Cluster) heartbeatLoop() {
	defer c.wg.Done()

	interval := c.opts.HeartbeatInterval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.writeHeartbeat(c.cancelCtx); err != nil {
				c.health.RecordFailure()
				c.logger.Warn("Cluster heartbeat write failed",
					mlog.String("node_id", c.node.ID),
					mlog.Err(err))
			}
		case <-c.cancelCtx.Done():
			return
		}
	}
}

// leaderLoop recomputes IsLeader every 5s. The interval is deliberately shorter
// than HeartbeatTTL so leader transfer after a node's TTL expiry happens in a
// bounded window. A newly-started node will compute itself leader on the first
// pass after it writes its own heartbeat (synchronous in StartInterNodeCommunication).
func (c *Cluster) leaderLoop() {
	defer c.wg.Done()

	const leaderCheckInterval = 5 * time.Second
	ticker := time.NewTicker(leaderCheckInterval)
	defer ticker.Stop()

	c.recomputeLeader()
	for {
		select {
		case <-ticker.C:
			c.recomputeLeader()
		case <-c.cancelCtx.Done():
			return
		}
	}
}

func (c *Cluster) recomputeLeader() {
	ids, err := c.scanAllNodeIDs(c.cancelCtx)
	if err != nil {
		// On scan failure we must not report leadership: if another node
		// succeeds its own scan, we'd risk duplicate leadership. Degrade
		// to non-leader so scheduled jobs simply don't run until Redis
		// recovers; better to miss a run than to double-run.
		c.logger.Warn("Leader SCAN failed, treating self as non-leader", mlog.Err(err))
		c.setLeader(false, "")
		return
	}
	if len(ids) == 0 {
		// No keys at all — unusual since our own heartbeat should be there.
		// Possible right after SCAN runs during key eviction; defer.
		c.setLeader(false, "")
		return
	}

	// Lexicographically smallest live node ID wins.
	leader := ids[0]
	for _, id := range ids[1:] {
		if id < leader {
			leader = id
		}
	}

	c.setLeader(leader == c.node.ID, leader)
}

// setLeader updates the cached leader state and invokes the listener chain
// on any transition. Routing every update through this helper ensures that
// both success- and failure-path demotions notify PlatformService listeners,
// which gate scheduled jobs.
func (c *Cluster) setLeader(isLeader bool, leaderID string) {
	was := c.leaderCache.Swap(isLeader)
	if was == isLeader {
		return
	}
	c.logger.Info("Cluster leader changed",
		mlog.String("self_id", c.node.ID),
		mlog.String("leader_id", leaderID),
		mlog.Bool("is_leader", isLeader))
	c.ps.InvokeClusterLeaderChangedListeners()
}

// scanAllNodeIDs walks every <prefix>:nodes:<id> key currently in Redis and
// returns the list of IDs (stripped of prefix). SCAN is used rather than KEYS
// to avoid blocking the Redis server under large deployments, even though in
// practice a cluster with thousands of nodes is not our target.
func (c *Cluster) scanAllNodeIDs(ctx context.Context) ([]string, error) {
	pattern := c.nodesKeyPattern()
	prefix := c.opts.KeyPrefix + ":nodes:"

	var cursor uint64
	var out []string
	for {
		cmd := c.cmdClient.B().Scan().
			Cursor(cursor).
			Match(pattern).
			Count(100).
			Build()

		ctx2, cancel := context.WithTimeout(ctx, 2*time.Second)
		resp := c.cmdClient.Do(ctx2, cmd)
		cancel()
		if err := resp.Error(); err != nil {
			return nil, err
		}

		entry, err := resp.AsScanEntry()
		if err != nil {
			return nil, err
		}
		for _, k := range entry.Elements {
			out = append(out, strings.TrimPrefix(k, prefix))
		}
		if entry.Cursor == 0 {
			break
		}
		cursor = entry.Cursor
	}
	return out, nil
}

// GetClusterInfos reads every live node key and decodes its ClusterInfo value.
// Used by the System Console "High Availability" page and by
// PlatformService.ClusterHealth logging. Returning an error here does not
// break the hot path, only the admin UI.
func (c *Cluster) GetClusterInfos() ([]*model.ClusterInfo, error) {
	ids, err := c.scanAllNodeIDs(c.cancelCtx)
	if err != nil {
		return nil, err
	}
	infos := make([]*model.ClusterInfo, 0, len(ids))
	for _, id := range ids {
		key := c.opts.KeyPrefix + ":nodes:" + id

		ctx2, cancel := context.WithTimeout(c.cancelCtx, 2*time.Second)
		resp := c.cmdClient.Do(ctx2, c.cmdClient.B().Get().Key(key).Build())
		cancel()
		if resp.Error() != nil {
			// Node may have expired between SCAN and GET; skip.
			continue
		}
		raw, err := resp.AsBytes()
		if err != nil {
			continue
		}
		var info model.ClusterInfo
		if err := json.Unmarshal(raw, &info); err != nil {
			c.logger.Warn("Failed to decode peer ClusterInfo", mlog.String("node_id", id), mlog.Err(err))
			continue
		}
		infos = append(infos, &info)
	}
	return infos, nil
}
