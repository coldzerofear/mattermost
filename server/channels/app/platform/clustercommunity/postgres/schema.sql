-- Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
-- See LICENSE.txt for license information.
--
-- Schema for the community PostgreSQL cluster backend. Applied idempotently
-- by postgres.Cluster.ensureSchema on every startup. Not part of Mattermost's
-- migration chain in channels/db/migrations/postgres/ on purpose, so that
-- community HA deployments don't collide with upstream schema evolution.

CREATE TABLE IF NOT EXISTS cluster_messages (
    id         VARCHAR(32) PRIMARY KEY,
    target     VARCHAR(64) NOT NULL DEFAULT '',
    payload    BYTEA       NOT NULL,
    created_at BIGINT      NOT NULL
);

-- Index for the GC DELETE. Without this, periodic cleanup would full-scan
-- the table under load.
CREATE INDEX IF NOT EXISTS idx_cluster_messages_created_at
    ON cluster_messages (created_at);
