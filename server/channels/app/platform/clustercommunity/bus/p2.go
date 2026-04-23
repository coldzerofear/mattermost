// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package bus

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/shared/mlog"
)

// P2Host is the shared dependency surface for the cluster-RPC-backed P2
// methods. Both redis.Cluster and postgres.Cluster embed / fulfil this
// interface, letting us implement ConfigChanged / GetClusterStats etc.
// once in bus/ and inherit the behavior from each backend.
//
// This is deliberately broader than Sender (which RPC uses): the P2 methods
// also need logging, config reload, and access to local data for responders.
//
// Note on naming: the Local* methods are what responders call to gather
// this-node data; the aggregating (cluster-wide) equivalents live on the
// ClusterInterface itself. Using distinct names prevents the hosting
// Cluster's interface methods from recursively calling themselves when
// they delegate to bus helpers.
type P2Host interface {
	Sender
	Log() mlog.LoggerIFace
	ReloadConfig() error
	LocalStats() *model.ClusterStats
	LocalWebConnCountForUser(userID string) int
	LocalPluginStatuses() (model.PluginStatuses, *model.AppError)
}

// RegisterConfigReload wires a node to both emit and react to the community
// config-reload event. Call once during StartInterNodeCommunication after
// the HandlerRegistry is available.
//
// Emit side: SendConfigReload below is the public entry point; it simply
// broadcasts the event. The actual config change persisted elsewhere; this
// message only says "re-read the shared store".
//
// React side: every peer that receives the event calls host.ReloadConfig.
// Failures are logged but do not propagate — one node failing to reload is
// not a reason to break the broadcaster's save path.
func RegisterConfigReload(host P2Host, handlers *HandlerRegistry) {
	handlers.RegisterClusterMessageHandler(ClusterEventCommunityConfigReload, func(msg *model.ClusterMessage) {
		if err := host.ReloadConfig(); err != nil {
			host.Log().Warn("Community cluster: ReloadConfig failed in response to peer config change",
				mlog.Err(err))
			return
		}
		host.Log().Info("Community cluster: reloaded config in response to peer broadcast")
	})
}

// SendConfigReload is called by ConfigChanged on the node that just saved
// new settings. It is a plain broadcast — no payload, no correlation. Peers
// react by calling PlatformService.ReloadConfig to pick up the change from
// the shared config store.
//
// sendToOtherServer mirrors the upstream ClusterInterface.ConfigChanged
// contract: when false, the caller does not want the change propagated
// (e.g. a local-only test override).
func SendConfigReload(host P2Host, sendToOtherServer bool) *model.AppError {
	if !sendToOtherServer {
		return nil
	}
	host.SendClusterMessage(&model.ClusterMessage{
		Event:    ClusterEventCommunityConfigReload,
		SendType: model.ClusterSendReliable,
	})
	return nil
}

// RegisterClusterStats wires both sides of the stats RPC: this node
// responds to peer requests with its own stats, AND accepts incoming
// responses for requests it broadcasts.
//
// Call once during StartInterNodeCommunication.
func RegisterClusterStats(host P2Host, rpc *RPC) {
	rpc.RegisterResponder(
		ClusterEventCommunityStatsRequest,
		ClusterEventCommunityStatsResponse,
		func(*model.ClusterMessage) []byte {
			local := host.LocalStats()
			data, err := json.Marshal(local)
			if err != nil {
				host.Log().Warn("Community cluster: failed to encode local stats",
					mlog.Err(err))
				return nil
			}
			return data
		},
	)
	rpc.RegisterAwaiter(ClusterEventCommunityStatsResponse)
}

// CollectClusterStats asks every peer for its stats and returns the
// aggregate. The local node's stats are always included, prepended to the
// peer responses, so GetClusterStats can return complete info even when
// peers are unreachable.
//
// expectedPeers is len(GetClusterInfos) - 1; if unknown, pass a generous
// upper bound (e.g. 32) and rely on the timeout.
func CollectClusterStats(
	host P2Host,
	rpc *RPC,
	expectedPeers int,
	timeout time.Duration,
) ([]*model.ClusterStats, *model.AppError) {
	out := []*model.ClusterStats{host.LocalStats()}

	if expectedPeers <= 0 {
		return out, nil
	}

	responses := rpc.Broadcast(
		ClusterEventCommunityStatsRequest,
		nil, // no request payload needed
		expectedPeers,
		timeout,
	)
	for _, resp := range responses {
		if len(resp.Data) == 0 {
			continue
		}
		var cs model.ClusterStats
		if err := json.Unmarshal(resp.Data, &cs); err != nil {
			host.Log().Warn("Community cluster: failed to decode peer stats",
				mlog.Err(err))
			continue
		}
		out = append(out, &cs)
	}
	return out, nil
}

// RegisterWebConnCount wires both sides of the per-user websocket count RPC.
// Caller supplies host (to read local counts) and an RPC instance. Must be
// called once at startup.
//
// Responders read the userID from the request payload (raw bytes, no
// framing — WebConnCount fires on every presence transition so we keep the
// wire format lean) and reply with the integer count as ASCII.
func RegisterWebConnCount(host P2Host, rpc *RPC) {
	rpc.RegisterResponder(
		ClusterEventCommunityWebConnCountRequest,
		ClusterEventCommunityWebConnCountResponse,
		func(req *model.ClusterMessage) []byte {
			userID := string(req.Data)
			n := host.LocalWebConnCountForUser(userID)
			return []byte(strconv.Itoa(n))
		},
	)
	rpc.RegisterAwaiter(ClusterEventCommunityWebConnCountResponse)
}

// CollectWebConnCount returns the combined count of active websocket
// connections for userID across ALL nodes EXCEPT self. The upstream caller
// in web_hub.go adds its own local count separately, so sending our own
// count would double-count it.
//
// Fast timeout: this is called on every last-connection-closed event and
// blocks before marking a user offline. 1s is generous enough for healthy
// clusters and short enough to avoid user-visible presence lag.
func CollectWebConnCount(
	host P2Host,
	rpc *RPC,
	userID string,
	expectedPeers int,
	timeout time.Duration,
) (int, *model.AppError) {
	if expectedPeers <= 0 {
		return 0, nil
	}

	responses := rpc.Broadcast(
		ClusterEventCommunityWebConnCountRequest,
		[]byte(userID),
		expectedPeers,
		timeout,
	)

	total := 0
	for _, resp := range responses {
		n, err := strconv.Atoi(string(resp.Data))
		if err != nil {
			host.Log().Warn("Community cluster: failed to parse peer webconn count",
				mlog.String("user_id", userID),
				mlog.String("raw", string(resp.Data)),
				mlog.Err(err))
			continue
		}
		total += n
	}
	return total, nil
}

// RegisterPluginStatuses wires both sides of the plugin-statuses RPC. Called
// once at startup. Responders call host.LocalPluginStatuses and JSON-encode
// the result; empty / error outcomes produce an empty response so the
// aggregator can always append safely.
func RegisterPluginStatuses(host P2Host, rpc *RPC) {
	rpc.RegisterResponder(
		ClusterEventCommunityPluginStatusesRequest,
		ClusterEventCommunityPluginStatusesResponse,
		func(*model.ClusterMessage) []byte {
			statuses, appErr := host.LocalPluginStatuses()
			if appErr != nil {
				host.Log().Warn("Community cluster: LocalPluginStatuses failed",
					mlog.Err(appErr))
				return nil
			}
			data, err := json.Marshal(statuses)
			if err != nil {
				host.Log().Warn("Community cluster: failed to encode plugin statuses",
					mlog.Err(err))
				return nil
			}
			return data
		},
	)
	rpc.RegisterAwaiter(ClusterEventCommunityPluginStatusesResponse)
}

// CollectPluginStatuses broadcasts the statuses request and concatenates
// every peer's reply. Upstream callers merge this with their own local
// statuses, so (like CollectWebConnCount) we don't include self here.
func CollectPluginStatuses(
	host P2Host,
	rpc *RPC,
	expectedPeers int,
	timeout time.Duration,
) (model.PluginStatuses, *model.AppError) {
	if expectedPeers <= 0 {
		return model.PluginStatuses{}, nil
	}

	responses := rpc.Broadcast(
		ClusterEventCommunityPluginStatusesRequest,
		nil,
		expectedPeers,
		timeout,
	)

	out := model.PluginStatuses{}
	for _, resp := range responses {
		if len(resp.Data) == 0 {
			continue
		}
		var statuses model.PluginStatuses
		if err := json.Unmarshal(resp.Data, &statuses); err != nil {
			host.Log().Warn("Community cluster: failed to decode peer plugin statuses",
				mlog.Err(err))
			continue
		}
		out = append(out, statuses...)
	}
	return out, nil
}
