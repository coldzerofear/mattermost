// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package bus

import (
	"net"
	"os"
	"sync/atomic"

	"github.com/mattermost/mattermost/server/public/model"
)

// NodeIdentity carries the stable identity of this process within the cluster.
// ID is used for loopback filtering and heartbeat keys; it must be unique
// across all replicas sharing the same backend.
type NodeIdentity struct {
	ID          string
	ClusterName string
	Hostname    string
	IPAddress   string
}

// ResolveNodeIdentity returns a NodeIdentity using, in order of preference:
//  1. MM_CLUSTER_NODE_ID env var (explicit override, e.g. K8s Pod name)
//  2. hostname + random suffix, so two processes on the same host don't clash
//
// Hostname / IPAddress are best-effort and may be empty on exotic environments.
func ResolveNodeIdentity(configured string, clusterName string) *NodeIdentity {
	hn, _ := os.Hostname()
	if hn == "" {
		hn = "mm"
	}

	id := configured
	if id == "" {
		id = hn + "-" + model.NewId()[:8]
	}

	return &NodeIdentity{
		ID:          id,
		ClusterName: clusterName,
		Hostname:    hn,
		IPAddress:   firstNonLoopbackIP(),
	}
}

func firstNonLoopbackIP() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return ""
	}
	for _, i := range ifaces {
		if i.Flags&net.FlagUp == 0 || i.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := i.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			var ip net.IP
			switch v := a.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			// Prefer IPv4 for readability in logs; fall back to IPv6 if no v4 exists.
			if ip.To4() != nil {
				return ip.String()
			}
		}
	}
	return ""
}

// HealthTracker counts consecutive failures of the backend transport
// (Redis publish/subscribe errors, PG notify errors, etc.). Score() feeds
// ClusterInterface.HealthScore where lower is better and 0 means healthy.
type HealthTracker struct {
	failures atomic.Int64
}

func (h *HealthTracker) RecordFailure() { h.failures.Add(1) }
func (h *HealthTracker) RecordSuccess() { h.failures.Store(0) }
func (h *HealthTracker) Score() int     { return int(h.failures.Load()) }
