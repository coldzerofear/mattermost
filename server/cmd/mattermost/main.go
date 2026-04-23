// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package main

import (
	"os"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/v8/cmd/mattermost/commands"
	// Import and register app layer slash commands
	_ "github.com/mattermost/mattermost/server/v8/channels/app/slashcommands"
	// Plugins
	_ "github.com/mattermost/mattermost/server/v8/channels/app/oauthproviders/gitlab"
	_ "github.com/mattermost/mattermost/server/v8/channels/app/oauthproviders/openid"

	// Enterprise Imports
	_ "github.com/mattermost/mattermost/server/v8/enterprise"

	// Community-edition cluster implementations (Redis / PostgreSQL).
	// Registers a ClusterInterface factory via init(); the factory is only
	// invoked when MM_CLUSTER_MODE is set to redis or postgres at runtime.
	_ "github.com/mattermost/mattermost/server/v8/channels/app/platform/clustercommunity"
)

func main() {
	model.BuildEnterpriseReady = "true"
	if err := commands.Run(os.Args[1:]); err != nil {
		os.Exit(1)
	}
}
