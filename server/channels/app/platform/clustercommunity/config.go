// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

// Package clustercommunity provides community-edition cluster implementations
// (Redis Pub/Sub and PostgreSQL LISTEN/NOTIFY) that fulfill einterfaces.ClusterInterface.
//
// Selection is driven by the MM_CLUSTER_MODE environment variable:
//
//	redis     -> Redis Pub/Sub backend
//	postgres  -> PostgreSQL LISTEN/NOTIFY backend (not implemented in this phase)
//	none|""   -> disabled (single-node behavior, identical to upstream)
package clustercommunity

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// ModeRedis / ModePostgres / ModeNone are the valid values for MM_CLUSTER_MODE.
const (
	ModeRedis    = "redis"
	ModePostgres = "postgres"
	ModeNone     = "none"
)

// Config is the runtime configuration parsed from environment variables.
// A single Config is shared by all backends; backend-specific fields live in
// the nested RedisConfig / PGConfig structs.
type Config struct {
	Mode              string
	NodeID            string
	ClusterName       string
	HeartbeatInterval time.Duration
	HeartbeatTTL      time.Duration

	Redis RedisConfig
	PG    PGConfig
}

type RedisConfig struct {
	Addr      string
	Password  string
	DB        int
	TLS       bool
	KeyPrefix string
}

type PGConfig struct {
	DSN         string
	ChannelName string

	// MaxConns controls the per-pod cluster DB connection pool size.
	// Total PG connections ≈ (MaxConns + 2) × replicaCount.
	// Set via MM_CLUSTER_PG_MAX_CONNS; default 10.
	MaxConns int

	// WebConnRPCTimeout is the deadline for WebConnCountForUser to collect
	// responses from all peers. Set via MM_CLUSTER_WEBCONN_RPC_TIMEOUT_MS
	// (milliseconds); default 1000ms.
	WebConnRPCTimeout time.Duration
}

// LoadConfigFromEnv reads MM_CLUSTER_* environment variables and returns a Config
// populated with defaults for any missing values. It never returns an error;
// callers validate Mode downstream.
func LoadConfigFromEnv() *Config {
	c := &Config{
		Mode:              strings.ToLower(getEnv("MM_CLUSTER_MODE", ModeNone)),
		NodeID:            getEnv("MM_CLUSTER_NODE_ID", ""),
		ClusterName:       getEnv("MM_CLUSTER_NAME", "mm-cluster"),
		HeartbeatInterval: getEnvSeconds("MM_CLUSTER_HEARTBEAT_INTERVAL", 10*time.Second),
		HeartbeatTTL:      getEnvSeconds("MM_CLUSTER_HEARTBEAT_TTL", 30*time.Second),
	}
	c.Redis = RedisConfig{
		Addr:      getEnv("MM_CLUSTER_REDIS_ADDR", ""),
		Password:  getEnv("MM_CLUSTER_REDIS_PASSWORD", ""),
		DB:        getEnvInt("MM_CLUSTER_REDIS_DB", 0),
		TLS:       strings.EqualFold(getEnv("MM_CLUSTER_REDIS_TLS", "false"), "true"),
		KeyPrefix: getEnv("MM_CLUSTER_REDIS_KEY_PREFIX", "mm:cluster"),
	}
	c.PG = PGConfig{
		DSN:               getEnv("MM_CLUSTER_PG_DSN", ""),
		ChannelName:       getEnv("MM_CLUSTER_PG_CHANNEL", "mm_cluster"),
		MaxConns:          getEnvInt("MM_CLUSTER_PG_MAX_CONNS", 10),
		WebConnRPCTimeout: getEnvMs("MM_CLUSTER_WEBCONN_RPC_TIMEOUT_MS", 1000),
	}
	return c
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getEnvSeconds(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return def
}

func getEnvMs(key string, defMs int) time.Duration {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Millisecond
		}
	}
	return time.Duration(defMs) * time.Millisecond
}
