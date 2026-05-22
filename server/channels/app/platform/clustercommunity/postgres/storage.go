// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package postgres

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/mattermost/mattermost/server/public/shared/mlog"
)

//go:embed schema.sql
var embeddedSchemaSQL string

// ensureSchema creates cluster_messages and its index if absent. Idempotent;
// invoked on every startup so fresh deployments work without a separate
// migration step.
func (c *Cluster) ensureSchema(ctx context.Context) error {
	if _, err := c.db.ExecContext(ctx, embeddedSchemaSQL); err != nil {
		return fmt.Errorf("ensure cluster_messages schema: %w", err)
	}
	return nil
}

// insertAndNotifyCTE folds INSERT + pg_notify into a single statement.
// PostgreSQL wraps any standalone statement in an implicit transaction, so
// the NOTIFY is queued and only released to subscribers when the implicit
// COMMIT happens — i.e. after the row is durably written. This eliminates
// three round-trips (BEGIN, separate NOTIFY, COMMIT) compared to the
// previous transaction-based implementation, cutting per-message latency
// from 4 RTT to 1 RTT.
const insertAndNotifyCTE = `
	WITH ins AS (
		INSERT INTO cluster_messages (id, target, payload, created_at)
		VALUES ($1, $2, $3, $4)
		RETURNING id
	)
	SELECT pg_notify($5, ins.id) FROM ins
`

// insertAndNotify writes a row and triggers a NOTIFY in a single round-trip.
func (c *Cluster) insertAndNotify(ctx context.Context, id, target string, payload []byte, createdAtMS int64) error {
	if _, err := c.db.ExecContext(ctx, insertAndNotifyCTE,
		id, target, payload, createdAtMS, c.opts.ChannelName,
	); err != nil {
		return fmt.Errorf("insert+notify: %w", err)
	}
	return nil
}

// fetchPayload returns the envelope bytes for the given message ID, filtering
// at the database layer for this node's visibility: broadcast (target='') or
// an explicit direct send (target=self). Returning (nil, nil) means the row
// exists but was not addressed to us — silently drop.
func (c *Cluster) fetchPayload(ctx context.Context, id string) ([]byte, error) {
	const q = `SELECT payload FROM cluster_messages
	           WHERE id = $1 AND (target = '' OR target = $2)`

	var payload []byte
	err := c.db.QueryRowContext(ctx, q, id, c.node.ID).Scan(&payload)
	if err != nil {
		// sql.ErrNoRows is the "not for us / already GC'd" case; treat as
		// a soft miss rather than an error. Upstream callers log at debug
		// level and move on.
		return nil, err
	}
	return payload, nil
}

// gcOldMessages deletes rows older than retention. Called periodically by
// gcLoop. Every node attempts this (not just leader) because the operation
// is idempotent and we'd rather have redundant cleanup than leave messages
// piling up if the leader is lost.
func (c *Cluster) gcOldMessages(ctx context.Context, retentionMS int64) {
	cutoff := nowMS() - retentionMS
	res, err := c.db.ExecContext(ctx,
		`DELETE FROM cluster_messages WHERE created_at < $1`, cutoff)
	if err != nil {
		c.logger.Warn("cluster_messages GC failed", mlog.Err(err))
		return
	}
	if n, _ := res.RowsAffected(); n > 0 {
		c.logger.Debug("cluster_messages GC",
			mlog.Int("removed", n),
			mlog.Int("cutoff_ms", cutoff))
	}
}
