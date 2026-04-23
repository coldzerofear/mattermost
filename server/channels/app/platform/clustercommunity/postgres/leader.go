// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package postgres

import (
	"context"
	"time"

	"github.com/mattermost/mattermost/server/public/shared/mlog"
)

// leaderLockKey is a 64-bit constant that uniquely identifies the Mattermost
// community HA leader lock within the PostgreSQL advisory-lock namespace.
// Chosen from the printable-ASCII high half so collisions with application
// code (which typically uses small numbers or hashes) are astronomically
// unlikely.
//
// Changing this value amounts to a flag day: every node in the cluster must
// use the same key, otherwise leadership will silently split.
const leaderLockKey int64 = 0x4d4d434c55535452 // "MMCLUSTR"

const (
	// leaderCheckInterval is how often non-leaders retry the acquire.
	// A shorter interval speeds up failover after the previous leader's
	// connection drops; 5s matches the Redis backend for consistency.
	leaderCheckInterval = 5 * time.Second

	// leaderAcquireTimeout caps each pg_try_advisory_lock call. The function
	// itself is non-blocking, but the surrounding Conn.Exec can still hang
	// on a saturated network. Fail fast and retry next tick.
	leaderAcquireTimeout = 3 * time.Second
)

// leaderLoop periodically attempts to acquire the advisory lock. PostgreSQL
// releases the lock automatically when the holding session disconnects, so
// crashed leaders never block successors — we never have to implement a
// separate expiry or fence.
//
// Key invariants:
//   - c.leaderConn, when non-nil, is a *live session* currently holding the
//     lock. Losing this connection (timeout, network, server restart) means
//     losing leadership.
//   - c.leaderCache tracks the last observed state; listeners are notified
//     on transition via setLeader.
func (c *Cluster) leaderLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(leaderCheckInterval)
	defer ticker.Stop()

	c.tryAcquireLeader()
	for {
		select {
		case <-ticker.C:
			c.tryAcquireLeader()
		case <-c.cancelCtx.Done():
			c.releaseLeader()
			return
		}
	}
}

func (c *Cluster) tryAcquireLeader() {
	c.leaderMu.Lock()
	defer c.leaderMu.Unlock()

	// Fast path: already leader. Verify the held connection is still alive
	// (Ping is cheap) and skip re-acquisition otherwise.
	if c.leaderConn != nil {
		ctx, cancel := context.WithTimeout(c.cancelCtx, leaderAcquireTimeout)
		err := c.leaderConn.PingContext(ctx)
		cancel()
		if err == nil {
			return
		}
		// Connection died — the lock is gone with it. Drop our state so
		// the next block can try to re-acquire.
		c.logger.Warn("Lost leader advisory lock connection; will retry",
			mlog.Err(err))
		_ = c.leaderConn.Close()
		c.leaderConn = nil
		c.setLeader(false)
	}

	// Acquisition attempt. Use a pinned Conn (not a pool-borrowed one) so
	// the lock's lifetime is bound to our Close.
	conn, err := c.db.Conn(c.cancelCtx)
	if err != nil {
		c.setLeader(false)
		return
	}

	ctx, cancel := context.WithTimeout(c.cancelCtx, leaderAcquireTimeout)
	defer cancel()

	var got bool
	if err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", leaderLockKey).Scan(&got); err != nil {
		_ = conn.Close()
		c.setLeader(false)
		return
	}

	if !got {
		// Some peer owns the lock — we are a follower. Release our Conn
		// back to the pool.
		_ = conn.Close()
		c.setLeader(false)
		return
	}

	c.leaderConn = conn
	c.setLeader(true)
}

// releaseLeader runs at shutdown. Although session close implicitly releases
// the lock, we call pg_advisory_unlock explicitly so peers see the
// relinquishment immediately instead of waiting for TCP FIN to propagate.
func (c *Cluster) releaseLeader() {
	c.leaderMu.Lock()
	defer c.leaderMu.Unlock()

	if c.leaderConn == nil {
		return
	}

	// Deliberately use Background here: cancelCtx is already cancelled.
	ctx, cancel := context.WithTimeout(context.Background(), leaderAcquireTimeout)
	defer cancel()

	if _, err := c.leaderConn.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", leaderLockKey); err != nil {
		c.logger.Warn("pg_advisory_unlock on shutdown failed", mlog.Err(err))
	}
	_ = c.leaderConn.Close()
	c.leaderConn = nil
	c.setLeader(false)
}

// setLeader records leader state and fires listeners on transition. Listeners
// are invoked in a separate goroutine by PlatformService, so holding
// leaderMu across the call is safe.
//
// Caller must hold c.leaderMu (or hold no exported state — only called from
// leaderLoop's goroutine).
func (c *Cluster) setLeader(isLeader bool) {
	was := c.leaderCache.Swap(isLeader)
	if was == isLeader {
		return
	}
	c.logger.Info("Postgres cluster leader changed",
		mlog.String("self_id", c.node.ID),
		mlog.Bool("is_leader", isLeader))
	c.ps.InvokeClusterLeaderChangedListeners()
}
