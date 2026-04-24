// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package postgres

import (
	"time"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/shared/mlog"
)

// discoveryType tags rows in the shared cluster_discovery table as owned by
// the community postgres backend. Upstream enterprise (gossip) uses its own
// type strings; keeping ours distinct prevents accidental cross-querying if
// a deployment ever mixes backends.
const discoveryType = "mm_cluster_postgres"

// gcInterval is how often each node attempts to purge expired rows from
// cluster_messages. Short enough to keep the table small under sustained
// load, long enough that every node's GC does not stampede.
const gcInterval = 30 * time.Second

// messageRetention is how long rows remain in cluster_messages after insert.
// Longer than any reasonable listener delay, short enough that the table
// stays small. 5 minutes covers the worst-case pq.Listener reconnect cycle
// (30s max) plus generous slack.
const messageRetention = 5 * time.Minute

// startHeartbeat delegates to PlatformService.NewClusterDiscoveryService,
// which already implements write-on-start + periodic ping + delete-on-stop
// against the cluster_discovery table. Reusing it avoids maintaining a
// second copy of the same logic.
//
// Tests construct Cluster with discoveryFactory = nil. In that case the
// heartbeat is skipped — the caller is presumably controlling the DB
// directly and does not need a ping goroutine.
//
// ClusterDiscovery.Id is validated by model.IsValidId to be a 26-char
// alphanumeric string (model.NewId format). Our bus node ID is free-form
// (hostname-<rand>, or whatever the operator set via MM_CLUSTER_NODE_ID
// such as a K8s Pod name), so we generate a dedicated Id for the
// heartbeat row. The two identifiers serve different purposes — bus node
// ID for loopback filtering on message envelopes, discovery row Id as an
// opaque primary key — and nothing in our code correlates them.
func (c *Cluster) startHeartbeat() {
	if c.discoveryFactory == nil {
		c.logger.Debug("Postgres cluster heartbeat skipped (no discovery factory wired)")
		return
	}
	ds := c.discoveryFactory()
	ds.ClusterDiscovery = model.ClusterDiscovery{
		Id:          model.NewId(),
		Type:        discoveryType,
		ClusterName: c.node.ClusterName,
		Hostname:    c.node.Hostname,
	}
	ds.AutoFillHostname()
	// IPAddress detection: ClusterDiscovery uses Hostname as display; IP is
	// filled opportunistically. NetworkInterface is left empty to match
	// enterprise defaults.
	ds.AutoFillIPAddress("", c.node.IPAddress)
	ds.Start()

	c.discovery = ds
}

// stopHeartbeat unblocks startHeartbeat's goroutine. Stop sends on an
// unbuffered channel, so it blocks until the ping loop has received and
// deleted our row. Safe to call exactly once per start.
func (c *Cluster) stopHeartbeat() {
	if c.discovery != nil {
		c.discovery.Stop()
		c.discovery = nil
	}
}

// gcLoop runs DELETE against old cluster_messages rows on a fixed interval.
// All nodes run this independently; the DELETE is idempotent so redundant
// runs are harmless. We do NOT gate on IsLeader because losing the leader
// while the table fills up is exactly when GC matters most.
func (c *Cluster) gcLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.gcOldMessages(c.cancelCtx, int64(messageRetention/time.Millisecond))
		case <-c.cancelCtx.Done():
			return
		}
	}
}

// listActiveNodes queries ClusterDiscoveryStore for peers of our Type in our
// ClusterName that have pinged recently (filter is applied inside GetAll
// via model.CDSOfflineAfterMillis). Used by GetClusterInfos.
//
// Tests that construct Cluster without a listNodesFunc get back (nil, nil),
// which GetClusterInfos turns into an empty slice.
func (c *Cluster) listActiveNodes() ([]*model.ClusterDiscovery, error) {
	if c.listNodesFunc == nil {
		return nil, nil
	}
	list, err := c.listNodesFunc(discoveryType, c.node.ClusterName)
	if err != nil {
		c.logger.Warn("ClusterDiscovery GetAll failed", mlog.Err(err))
		return nil, err
	}
	return list, nil
}
