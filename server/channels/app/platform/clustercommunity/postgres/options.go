// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

// Package postgres implements einterfaces.ClusterInterface on top of
// PostgreSQL's LISTEN/NOTIFY mechanism. It is the "no new dependencies"
// alternative to the Redis backend: every Mattermost deployment already has
// a Postgres instance, so enabling clustering requires nothing new to run.
//
// Message flow:
//
//	Publish  -> INSERT INTO cluster_messages (id, target, payload, created_at)
//	            SELECT pg_notify('<channel>', <id>)
//	            COMMIT                   (NOTIFY is deferred to commit time)
//	Receive  -> pq.Listener delivers *Notification{Extra: <id>}
//	            SELECT payload FROM cluster_messages WHERE id = <id> AND
//	            (target = '' OR target = <self>)
//	            bus.Decode(payload) -> Dispatch
//	Heartbeat-> PlatformService.NewClusterDiscoveryService() (shared with upstream)
//	Leader   -> pg_try_advisory_lock held on a dedicated session connection;
//	            PostgreSQL releases the lock automatically on disconnect.
package postgres

import "time"

// Options mirrors clustercommunity.PGConfig plus shared fields. Kept in-package
// so postgres.New does not need to import the parent package and risk cycles.
type Options struct {
	NodeID            string
	ClusterName       string
	HeartbeatInterval time.Duration
	HeartbeatTTL      time.Duration

	// DSN overrides the Mattermost main datasource. Empty means "use
	// ps.Config().SqlSettings.DataSource". Using the same database is
	// the common case because LISTEN/NOTIFY does not cross databases.
	DSN string

	// ChannelName is the PostgreSQL NOTIFY channel. All cluster nodes must
	// agree. Defaults to "mm_cluster" when empty.
	ChannelName string

	// MaxConns is the maximum number of open connections in the cluster DB
	// pool per pod. Total PostgreSQL connections ≈ (MaxConns + 2) × podCount.
	// Tune this down when running many pods to stay within PostgreSQL
	// max_connections. Defaults to 10 when zero or negative.
	MaxConns int

	// WebConnRPCTimeout caps how long WebConnCountForUser waits for all peers
	// to respond before returning the partial count. Shorter values give faster
	// offline-status transitions at the cost of missing slow pods. Default 1s.
	WebConnRPCTimeout time.Duration
}
