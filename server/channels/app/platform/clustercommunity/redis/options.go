// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

// Package redis implements einterfaces.ClusterInterface on top of Redis Pub/Sub
// for broadcast and SET-with-TTL for node discovery. It is the primary cluster
// backend for community-edition high-availability deployments.
//
// Message flow:
//
//	Publish  -> PUBLISH <prefix>:broadcast <json envelope>
//	Direct   -> PUBLISH <prefix>:node:<target_id> <json envelope>
//	Receive  -> SUBSCRIBE <prefix>:broadcast, <prefix>:node:<self_id>
//	Heartbeat-> SET <prefix>:nodes:<id> <clusterinfo json> EX <ttl>
//	Leader   -> the node with the lexicographically smallest live nodes:<id> key
package redis

import "time"

// Options is the redis-backend subset of the top-level Config. Kept separate
// so this package does not need to import the parent and to keep the public
// surface small.
type Options struct {
	NodeID            string
	ClusterName       string
	HeartbeatInterval time.Duration
	HeartbeatTTL      time.Duration

	Addr      string
	Password  string
	DB        int
	TLS       bool
	KeyPrefix string
}
