package license_impl

import (
	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/v8/channels/app/platform"
	"github.com/mattermost/mattermost/server/v8/einterfaces"
)

func init() {
	platform.RegisterLicenseInterface(func(ps *platform.PlatformService) einterfaces.LicenseInterface {
		return New(ps)
	})
}

type LicenseInterfaceImpl struct {
	ps *platform.PlatformService
}

func New(ps *platform.PlatformService) *LicenseInterfaceImpl {
	return &LicenseInterfaceImpl{
		ps: ps,
	}
}

func (l *LicenseInterfaceImpl) CanStartTrial() (bool, error) {
	return true, nil
}

func (l *LicenseInterfaceImpl) GetPrevTrial() (*model.License, error) {
	return l.ps.License(), nil
}

func (l *LicenseInterfaceImpl) NewMattermostEntryLicense(serverId string) *model.License {
	return l.ps.License()
}
