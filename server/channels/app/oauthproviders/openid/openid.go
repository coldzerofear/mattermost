// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package oauthopenid

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/shared/mlog"
	"github.com/mattermost/mattermost/server/public/shared/request"
	"github.com/mattermost/mattermost/server/v8/einterfaces"
)

//	"OpenIdSettings": {
//	    "Enable": true,
//	    "Secret": "fedcba987654321fedcba987654321",
//	    "Id": "123456789abcdef123456789abcdef",
//	    "Scope": "openid profile email",
//	    "AuthEndpoint": "https://<HOSTNAME>/oauth/authorize",
//	    "TokenEndpoint": "https://<HOSTNAME>/oauth/token",
//	    "UserApiEndpoint": "https://<HOSTNAME>/oauth/resource"
//	},
type OpenidProvider struct{}

type UserInfo struct {
	Sub   string `json:"sub"`
	Name  string `json:"name"`
	Email string `json:"email"`
	From  string `json:"from"`
}

func init() {
	einterfaces.RegisterOAuthProvider(model.ServiceOpenid, &OpenidProvider{})
}

func userFromUserInfo(logger mlog.LoggerIFace, info *UserInfo) *model.User {
	user := &model.User{}
	username := info.Sub
	if username == "" {
		username = info.Email
	}
	user.Username = model.CleanUsername(logger, username)
	splitName := strings.Split(info.Name, " ")
	if len(splitName) == 2 {
		user.FirstName = splitName[0]
		user.LastName = splitName[1]
	} else if len(splitName) >= 2 {
		user.FirstName = splitName[0]
		user.LastName = strings.Join(splitName[1:], " ")
	} else {
		user.FirstName = info.Name
	}
	user.Email = strings.ToLower(info.Email)
	userId := info.getAuthData()
	user.AuthData = &userId
	user.AuthService = model.ServiceOpenid

	return user
}

func (u *UserInfo) IsValid() error {
	if strings.TrimSpace(u.Sub) == "" {
		return fmt.Errorf("user sub can't be for '%s'", u.Sub)
	}
	if u.Email == "" {
		return errors.New("user e-mail should not be empty")
	}
	return nil
}

func (u *UserInfo) getAuthData() string {
	if len(u.From) == 0 {
		return fmt.Sprintf("%s-%s", model.ServiceOpenid, u.Sub)
	}
	return fmt.Sprintf("%s-%s", u.From, u.Sub)
}

func (op *OpenidProvider) GetSSOSettings(_ request.CTX, config *model.Config, service string) (*model.SSOSettings, error) {
	return &config.OpenIdSettings, nil
}

func (op *OpenidProvider) GetUserFromIdToken(_ request.CTX, idToken string) (*model.User, error) {
	return nil, nil
}

func (op *OpenidProvider) GetUserFromJSON(rctx request.CTX, data io.Reader, tokenUser *model.User) (*model.User, error) {
	decoder := json.NewDecoder(data)
	var info UserInfo
	err := decoder.Decode(&info)
	if err != nil {
		return nil, err
	}
	if err = info.IsValid(); err != nil {
		return nil, err
	}
	return userFromUserInfo(rctx.Logger(), &info), nil
}

func (op *OpenidProvider) IsSameUser(_ request.CTX, dbUser, oauthUser *model.User) bool {
	return dbUser.AuthData == oauthUser.AuthData
}
