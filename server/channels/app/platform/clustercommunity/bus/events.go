// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package bus

import "github.com/mattermost/mattermost/server/public/model"

// Community-edition cluster events. These are event names that only the
// community HA backends (redis / postgres) produce and consume; they do not
// appear in model.ClusterEvent because upstream enterprise does not need them
// (gossip handles the equivalent concerns internally).
//
// Prefixing with "mm_community_" keeps these out of the upstream namespace
// and avoids any risk of accidentally overriding a future core event name.
const (
	// ClusterEventCommunityConfigReload is broadcast by a node that just
	// successfully persisted a config change via the System Console. Every
	// peer that receives it calls PlatformService.ReloadConfig to pick up
	// the change from the shared config store.
	//
	// Payload: empty. The new config is already in the shared store; there
	// is no need to include it in the message, and sending it would risk
	// drift if the message races with another writer.
	ClusterEventCommunityConfigReload model.ClusterEvent = "mm_community_config_reload"

	// Request/response pair for GetClusterStats. Used by the community RPC
	// helper (see rpc.go) to fan out to peers and collect per-node stats.
	ClusterEventCommunityStatsRequest  model.ClusterEvent = "mm_community_stats_request"
	ClusterEventCommunityStatsResponse model.ClusterEvent = "mm_community_stats_response"

	// Request/response pair for WebConnCountForUser. Request payload = userID
	// as raw bytes; response payload = base-10 ASCII integer. This is the
	// hot-path message (fires on every last-connection-closed event) so we
	// keep the encoding cheap and skip JSON.
	ClusterEventCommunityWebConnCountRequest  model.ClusterEvent = "mm_community_webconn_count_request"
	ClusterEventCommunityWebConnCountResponse model.ClusterEvent = "mm_community_webconn_count_response"

	// Request/response pair for GetPluginStatuses. Request has no payload;
	// response is JSON(model.PluginStatuses). Called from the System Console
	// plugin listing page so volume is low and JSON overhead is acceptable.
	ClusterEventCommunityPluginStatusesRequest  model.ClusterEvent = "mm_community_plugin_statuses_request"
	ClusterEventCommunityPluginStatusesResponse model.ClusterEvent = "mm_community_plugin_statuses_response"
)
