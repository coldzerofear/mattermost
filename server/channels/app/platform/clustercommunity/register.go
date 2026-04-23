// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package clustercommunity

import (
	"github.com/mattermost/mattermost/server/v8/channels/app/platform"
)

// init wires the community cluster factory into PlatformService. It runs once
// per process when any code blank-imports this package. The target import
// point is server/cmd/mattermost/main.go:
//
//	_ "github.com/mattermost/mattermost/server/v8/channels/app/platform/clustercommunity"
//
// RegisterClusterInterface stores the factory pointer; the factory is only
// invoked later when PlatformService.initEnterprise runs, so reading
// environment variables inside New() sees the final process environment.
func init() {
	platform.RegisterClusterInterface(New)
}
