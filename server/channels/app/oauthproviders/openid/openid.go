// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package oauthopenid

import (
	"bytes"
	"crypto/rand"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

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
type OpenidProvider struct {
	numGen *NumberGenerator
}

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
	val := os.Getenv("SYSTEM_CODE")
	if val == "" {
		val = "00"
	}
	einterfaces.RegisterOAuthProvider(model.ServiceOpenid, &OpenidProvider{numGen: NewNumberGenerator(val)})
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

type IDTokenResponse struct {
	Head struct {
		Code    string `json:"code"` // 00000000 为成功 其余为失败
		Message string `json:"message"`
	} `json:"sysRspHead"`
	Data struct {
		ServerIp    string `json:"serverIp"`
		ReturnTime  string `json:"returnTime"` // 返回时间
		ServcSeqNo  string `json:"servcSeqNo"` // 流水号
		RecordCount int    `json:"recordCount"`
	} `json:"respData"`
	Body struct {
		EmpId        string     `json:"empId"`        // 员工编号
		CertTp       string     `json:"certTp"`       // 身份证类别
		CertNo       string     `json:"certNo"`       // 身份证号
		MoblNo       string     `json:"moblNo"`       // 手机号
		TellerNo     string     `json:"tellerNo"`     // 柜员号
		EmpNm        string     `json:"empNm"`        // 员工姓名
		BelgOrg      string     `json:"belgOrg"`      // 归属机构
		ListMainPost []MainPost `json:"listMainPost"` // 员工认证主岗位列表
	} `json:"body"`
}

func (op *OpenidProvider) GetUserFromIdToken(ctx request.CTX, idToken string) (*model.User, error) {
	if os.Getenv("ENABLED_OPENID_ID_TOKEN") != "true" {
		return nil, nil
	}
	endpoint := os.Getenv("ID_TOKEN_API_ENDPOINT")
	if endpoint == "" {
		return nil, fmt.Errorf("ID_TOKEN_API_ENDPOINT is empty")
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("invalid endpoint scheme: %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("invalid endpoint host")
	}

	header := map[string]string{}
	if val := os.Getenv("ID_TOKEN_HEADER_SET"); val != "" {
		if err := json.Unmarshal([]byte(val), &header); err != nil {
			return nil, err
		}
	}
	next := op.numGen.Next()
	bodymap := map[string]interface{}{
		"ctrlData": map[string]interface{}{
			// TODO 这些值暂时忽略
			//"appPushId":            "",
			//"transDesc":            "",
			//"pageIndex":            1,
			//"pageSize":             200,
			//"authTransAmtSuccFlag": "",
			//"authTransSuccFlag":    "",
			//"tellerSeqNo":          "",
			//"transTeller":          "000000",
			//"userId":               "000000",
			//"termId":               "000000",

			"hostIp":       "0.0.0.0",
			"transBranch":  "000000",
			"servRemark":   "asUeccSsoCheckToken",
			"servcId":      "asUeccSsoCheckToken",
			"reqTime":      time.Now().Format(time.DateTime),
			"transMedium":  "BM",
			"sysIndicator": "8B",
			"transSeqNo":   next, // <应用系统代码2位><日期标识符，格式yyyymmdd><流水号10位>  一共20位
			"bizTrackNo":   next, // <发起系统代码2位><日期标识符，格式yyyymmdd><流水号10位>  一共20位
		},
		"body": map[string]string{
			"authToken": idToken,
			"prvlgFlg":  "1",
		},
	}
	marshal, err := json.Marshal(bodymap)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx.Context(), http.MethodPost, endpoint, bytes.NewReader(marshal))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	for key, val := range header {
		req.Header.Set(key, val)
	}
	httpClient := &http.Client{
		Timeout: 15 * time.Second,
	}
	if u.Scheme == "https" {
		httpClient.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	data := IDTokenResponse{}
	if err = json.Unmarshal(respBody, &data); err != nil {
		return nil, err
	}
	if data.Head.Code != "00000000" {
		return nil, errors.New(data.Head.Message)
	}
	info := JYUserInfo{
		Status:     data.Head.Code,
		Message:    data.Head.Message,
		ReturnTime: data.Data.ReturnTime,
		ServcSeqNo: data.Data.ServcSeqNo,
		Success:    true,
	}
	info.Data.EmpId = data.Body.EmpId
	info.Data.CertTp = data.Body.CertTp
	info.Data.CertNo = data.Body.CertNo
	info.Data.MoblNo = data.Body.MoblNo
	info.Data.TellerNo = data.Body.TellerNo
	info.Data.EmpNm = data.Body.EmpNm
	info.Data.BelgOrg = data.Body.BelgOrg
	info.Data.ListMainPost = data.Body.ListMainPost
	if err = info.IsValid(); err != nil {
		return nil, err
	}
	return userFromJYUserInfo(ctx.Logger(), &info), nil
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

const maxSeq uint64 = 10000000000 // 10^10

type NumberGenerator struct {
	mu sync.Mutex

	systemCode string // 2位数字

	weekKey int    // 当前周标识，例如 202614
	base    uint64 // 本周随机起点
	count   uint64 // 本周已生成数量

	// 扰动参数，A 必须与 10^10 互质（不能被2和5整除）
	a uint64
	b uint64
}

func NewNumberGenerator(systemCode string) *NumberGenerator {
	if len(systemCode) != 2 || !isDigits(systemCode) {
		panic("system code must be exactly 2 digits")
	}

	now := time.Now()
	year, week := now.ISOWeek()

	return &NumberGenerator{
		systemCode: systemCode,
		weekKey:    year*100 + week,
		base:       randomUint64(maxSeq),
		count:      0,
		a:          3141592651, // 与 10^10 互质
		b:          2718281828,
	}
}

// Next 生成固定20位号码：
// <2位系统代码><8位日期yyyymmdd><10位流水号>
func (g *NumberGenerator) Next() string {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now()
	year, week := now.ISOWeek()
	currentWeekKey := year*100 + week

	// 跨周后重置
	if currentWeekKey != g.weekKey {
		g.weekKey = currentWeekKey
		g.base = randomUint64(maxSeq)
		g.count = 0
	}

	// 理论上 1 周最多可生成 10^10 个，不太可能打满
	// 如果真打满，就阻塞到下周，而不是返回错误
	if g.count >= maxSeq {
		for {
			g.mu.Unlock()
			time.Sleep(time.Second)
			g.mu.Lock()

			now = time.Now()
			year, week = now.ISOWeek()
			currentWeekKey = year*100 + week
			if currentWeekKey != g.weekKey {
				g.weekKey = currentWeekKey
				g.base = randomUint64(maxSeq)
				g.count = 0
				break
			}
		}
	}

	raw := (g.base + g.count) % maxSeq
	g.count++

	// 做一层可逆扰动，让结果不显得是简单递增
	seq := (g.a*raw + g.b) % maxSeq

	result := fmt.Sprintf("%s%s%010d",
		g.systemCode,
		now.Format("20060102"),
		seq,
	)

	// 这里长度恒为20
	return result
}

func randomUint64(max uint64) uint64 {
	if max == 0 {
		return 0
	}
	n, err := rand.Int(rand.Reader, big.NewInt(int64(max)))
	if err != nil {
		// 不返回错误，退化成时间种子方案
		return uint64(time.Now().UnixNano()) % max
	}
	return uint64(n.Int64())
}

func isDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
