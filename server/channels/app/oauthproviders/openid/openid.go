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

type LRUserInfo struct {
	Sub   string `json:"sub"`
	Name  string `json:"name"`
	Email string `json:"email"`
	From  string `json:"from"`
}

func (u *LRUserInfo) IsValid() error {
	if strings.TrimSpace(u.Sub) == "" {
		return fmt.Errorf("user sub can't be for '%s'", u.Sub)
	}
	if u.Email == "" {
		return errors.New("user e-mail should not be empty")
	}
	return nil
}

func (u *LRUserInfo) getAuthData() string {
	return fmt.Sprintf("%s-%s", model.ServiceOpenid, u.Sub)
}

type MainPost struct {
	PostCd string `json:"postCd"` //主岗位编号
	PostNm string `json:"postNm"` // 主岗位名称
}

type JYUserInfo struct {
	Status     string `json:"status"`     // 00000000 为成功 其余为失败
	Message    string `json:"message"`    // 返回信息
	ReturnTime string `json:"returnTime"` // 返回时间
	ServcSeqNo string `json:"servcSeqNo"` // 流水号
	Success    bool   `json:"success"`
	Data       struct {
		EmpId        string     `json:"empId"`        // 员工编号
		CertTp       string     `json:"certTp"`       // 身份证类别
		CertNo       string     `json:"certNo"`       // 身份证号
		MoblNo       string     `json:"moblNo"`       // 手机号
		TellerNo     string     `json:"tellerNo"`     // 柜员号
		EmpNm        string     `json:"empNm"`        // 员工姓名
		BelgOrg      string     `json:"belgOrg"`      // 归属机构
		Env          int        `json:"env"`          // 当前用户登录环境 0-开发网 1-互联网 2-OA网
		ListMainPost []MainPost `json:"listMainPost"` // 员工认证主岗位列表
	} `json:"data"`
}

func (u *JYUserInfo) IsValid() error {
	if !u.Success || u.Status != "00000000" {
		return errors.New(u.Message)
	}
	if u.Data.EmpId == "" {
		return errors.New("user emp-id should not be empty")
	}
	//if u.Data.MoblNo == "" {
	//	return errors.New("user mobile phone number should not be empty")
	//}
	return nil
}

func (u *JYUserInfo) getAuthData() string {
	return fmt.Sprintf("%s-%s", model.ServiceOpenid, u.Data.EmpId)
}

func init() {
	einterfaces.RegisterOAuthProvider(model.ServiceOpenid, &OpenidProvider{})
}

func userFromLRUserInfo(logger mlog.LoggerIFace, info *LRUserInfo) *model.User {
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

func userFromJYUserInfo(logger mlog.LoggerIFace, info *JYUserInfo) *model.User {
	user := &model.User{}
	user.Username = model.CleanUsername(logger, info.Data.EmpId)
	splitName := strings.Split(info.Data.EmpNm, " ")
	if len(splitName) == 2 {
		user.FirstName = splitName[0]
		user.LastName = splitName[1]
	} else if len(splitName) >= 2 {
		user.FirstName = splitName[0]
		user.LastName = strings.Join(splitName[1:], " ")
	} else {
		user.FirstName = info.Data.EmpNm
	}
	if info.Data.MoblNo != "" {
		user.Props = model.StringMap{
			"MOBL_NO": info.Data.MoblNo,
		}
	}
	user.Email = strings.ToLower(info.Data.EmpId + "@zjrcu.com")
	userId := info.getAuthData()
	user.AuthData = &userId
	user.AuthService = model.ServiceOpenid
	return user
}

func (op *OpenidProvider) GetSSOSettings(_ request.CTX, config *model.Config, service string) (*model.SSOSettings, error) {
	return &config.OpenIdSettings, nil
}

func (op *OpenidProvider) GetUserFromIdToken(_ request.CTX, idToken string) (*model.User, error) {
	return nil, nil
}

func (op *OpenidProvider) GetUserFromJSON(rctx request.CTX, data io.Reader, tokenUser *model.User) (*model.User, error) {
	raw, err := io.ReadAll(data)
	if err != nil {
		return nil, err
	}

	var lrInfo LRUserInfo
	if err = json.Unmarshal(raw, &lrInfo); err == nil {
		if err = lrInfo.IsValid(); err == nil {
			return userFromLRUserInfo(rctx.Logger(), &lrInfo), nil
		}
	}

	var jyInfo JYUserInfo
	if err = json.Unmarshal(raw, &jyInfo); err == nil {
		if err = jyInfo.IsValid(); err == nil {
			return userFromJYUserInfo(rctx.Logger(), &jyInfo), nil
		}
	}
	return nil, err
}

func (op *OpenidProvider) IsSameUser(_ request.CTX, dbUser, oauthUser *model.User) bool {
	// TODO 当用户没绑定认证服务时通过对比email确认是用一个用户
	if dbUser.AuthService == "" {
		return dbUser.Email == oauthUser.Email
	}
	return dbUser.AuthData == oauthUser.AuthData
}
