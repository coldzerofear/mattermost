// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package clustercommunity

import (
	"github.com/mattermost/mattermost/server/public/shared/mlog"
	"github.com/mattermost/mattermost/server/v8/channels/app/platform"
	"github.com/mattermost/mattermost/server/v8/channels/app/platform/clustercommunity/postgres"
	"github.com/mattermost/mattermost/server/v8/channels/app/platform/clustercommunity/redis"
	"github.com/mattermost/mattermost/server/v8/einterfaces"
)

// New is the factory passed to platform.RegisterClusterInterface. It inspects
// MM_CLUSTER_MODE and constructs the selected backend. Returning nil is the
// explicit signal for "no cluster" and makes PlatformService behave exactly
// like upstream community edition.
//
// Any backend construction error is logged but NOT propagated: we fall back
// to single-node mode rather than preventing server startup. A broken Redis
// should not take down Mattermost.
func New(ps *platform.PlatformService) einterfaces.ClusterInterface {
	cfg := LoadConfigFromEnv()
	logger := ps.Log()

	switch cfg.Mode {
	case ModeRedis:
		c, err := redis.New(ps, redisAdapter(cfg))
		if err != nil {
			logger.Error("Failed to initialize Redis cluster backend, falling back to single-node mode",
				mlog.Err(err),
				mlog.String("redis_addr", cfg.Redis.Addr),
			)
			return nil
		}
		logger.Info("Community cluster: Redis backend initialized",
			mlog.String("node_id", c.NodeID()),
			mlog.String("redis_addr", cfg.Redis.Addr),
		)
		return c

	case ModePostgres:
		c, err := postgres.NewFromPlatform(ps, postgresAdapter(cfg))
		if err != nil {
			logger.Error("Failed to initialize Postgres cluster backend, falling back to single-node mode",
				mlog.Err(err),
				mlog.Bool("pg_dsn_explicit", cfg.PG.DSN != ""),
			)
			return nil
		}
		logger.Info("Community cluster: Postgres backend initialized",
			mlog.String("node_id", c.NodeID()),
			mlog.String("channel", cfg.PG.ChannelName),
		)
		return c

	case ModeNone, "":
		return nil

	default:
		logger.Error("Community cluster: unknown MM_CLUSTER_MODE, falling back to single-node mode",
			mlog.String("mode", cfg.Mode),
		)
		return nil
	}
}

// redisAdapter decouples the Redis backend from the top-level Config struct.
// The Redis package does not need to know anything about other backends.
func redisAdapter(c *Config) *redis.Options {
	return &redis.Options{
		NodeID:            c.NodeID,
		ClusterName:       c.ClusterName,
		HeartbeatInterval: c.HeartbeatInterval,
		HeartbeatTTL:      c.HeartbeatTTL,
		Addr:              c.Redis.Addr,
		Password:          c.Redis.Password,
		DB:                c.Redis.DB,
		TLS:               c.Redis.TLS,
		KeyPrefix:         c.Redis.KeyPrefix,
	}
}

// postgresAdapter is the analogue of redisAdapter for the PG backend.
func postgresAdapter(c *Config) *postgres.Options {
	return &postgres.Options{
		NodeID:            c.NodeID,
		ClusterName:       c.ClusterName,
		HeartbeatInterval: c.HeartbeatInterval,
		HeartbeatTTL:      c.HeartbeatTTL,
		DSN:               c.PG.DSN,
		ChannelName:       c.PG.ChannelName,
		MaxConns:          c.PG.MaxConns,
		WebConnRPCTimeout: c.PG.WebConnRPCTimeout,
	}
}
