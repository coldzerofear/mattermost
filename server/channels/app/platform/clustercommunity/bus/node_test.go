// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package bus

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveNodeIdentityExplicit(t *testing.T) {
	// When MM_CLUSTER_NODE_ID is provided (e.g. K8s Pod name), we must
	// use it verbatim. Regenerating per-process would cause Pod restarts
	// to appear as new cluster members every time.
	id := ResolveNodeIdentity("explicit-id", "testcluster")
	require.Equal(t, "explicit-id", id.ID)
	require.Equal(t, "testcluster", id.ClusterName)
}

func TestResolveNodeIdentityGenerated(t *testing.T) {
	id1 := ResolveNodeIdentity("", "cluster")
	id2 := ResolveNodeIdentity("", "cluster")

	require.NotEmpty(t, id1.ID)
	require.NotEmpty(t, id2.ID)
	require.NotEqual(t, id1.ID, id2.ID, "two generated IDs must differ")

	// Format sanity: should start with hostname-ish prefix, contain a dash,
	// and end with 8 id chars. We don't assert the exact hostname because
	// the test runner's hostname is environment-specific.
	require.True(t, strings.Contains(id1.ID, "-"), "expected hostname-<rand>, got %s", id1.ID)
	require.Greater(t, len(id1.ID), 8)
}

func TestHealthTrackerTransitions(t *testing.T) {
	h := &HealthTracker{}
	require.Equal(t, 0, h.Score(), "new tracker starts healthy")

	h.RecordFailure()
	h.RecordFailure()
	h.RecordFailure()
	require.Equal(t, 3, h.Score())

	h.RecordSuccess()
	require.Equal(t, 0, h.Score(), "success must clear the failure counter")
}
