// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package app

import (
	"mime/multipart"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/v8/einterfaces"
	ejobs "github.com/mattermost/mattermost/server/v8/einterfaces/jobs"
)

var accountMigrationInterface func(*App) einterfaces.AccountMigrationInterface

func RegisterAccountMigrationInterface(f func(*App) einterfaces.AccountMigrationInterface) {
	accountMigrationInterface = f
}

var complianceInterface func(*App) einterfaces.ComplianceInterface

func RegisterComplianceInterface(f func(*App) einterfaces.ComplianceInterface) {
	complianceInterface = f
}

var dataRetentionInterface func(*App) einterfaces.DataRetentionInterface

func RegisterDataRetentionInterface(f func(*App) einterfaces.DataRetentionInterface) {
	dataRetentionInterface = f
}

var jobsDataRetentionJobInterface func(*Server) ejobs.DataRetentionJobInterface

func RegisterJobsDataRetentionJobInterface(f func(*Server) ejobs.DataRetentionJobInterface) {
	jobsDataRetentionJobInterface = f
}

var jobsMessageExportJobInterface func(*Server) ejobs.MessageExportJobInterface

func RegisterJobsMessageExportJobInterface(f func(*Server) ejobs.MessageExportJobInterface) {
	jobsMessageExportJobInterface = f
}

var jobsElasticsearchAggregatorInterface func(*Server) ejobs.ElasticsearchAggregatorInterface

func RegisterJobsElasticsearchAggregatorInterface(f func(*Server) ejobs.ElasticsearchAggregatorInterface) {
	jobsElasticsearchAggregatorInterface = f
}

var jobsElasticsearchIndexerInterface func(*Server) ejobs.IndexerJobInterface

func RegisterJobsElasticsearchIndexerInterface(f func(*Server) ejobs.IndexerJobInterface) {
	jobsElasticsearchIndexerInterface = f
}

var jobsLdapSyncInterface func(*App) ejobs.LdapSyncInterface

func RegisterJobsLdapSyncInterface(f func(*App) ejobs.LdapSyncInterface) {
	jobsLdapSyncInterface = f
}

var ldapInterface func(*App) einterfaces.LdapInterface

func RegisterLdapInterface(f func(*App) einterfaces.LdapInterface) {
	ldapInterface = f
}

var messageExportInterface func(*App) einterfaces.MessageExportInterface

func RegisterMessageExportInterface(f func(*App) einterfaces.MessageExportInterface) {
	messageExportInterface = f
}

var cloudInterface func(*Server) einterfaces.CloudInterface

func RegisterCloudInterface(f func(*Server) einterfaces.CloudInterface) {
	cloudInterface = f
}

var samlInterface func(*App) einterfaces.SamlInterface

func RegisterSamlInterface(f func(*App) einterfaces.SamlInterface) {
	samlInterface = f
}

var notificationInterface func(*App) einterfaces.NotificationInterface

func RegisterNotificationInterface(f func(*App) einterfaces.NotificationInterface) {
	notificationInterface = f
}

var outgoingOauthConnectionInterface func(*App) einterfaces.OutgoingOAuthConnectionInterface

func RegisterOutgoingOAuthConnectionInterface(f func(*App) einterfaces.OutgoingOAuthConnectionInterface) {
	outgoingOauthConnectionInterface = f
}

var ipFilteringInterface func(*App) einterfaces.IPFilteringInterface

func RegisterIPFilteringInterface(f func(*App) einterfaces.IPFilteringInterface) {
	ipFilteringInterface = f
}

var accessControlServiceInterface func(*App) einterfaces.AccessControlServiceInterface

func RegisterAccessControlServiceInterface(f func(*App) einterfaces.AccessControlServiceInterface) {
	accessControlServiceInterface = f
}

var jobsAccessControlSyncJobInterface func(*Server) ejobs.AccessControlSyncJobInterface

func RegisterJobsAccessControlSyncJobInterface(f func(*Server) ejobs.AccessControlSyncJobInterface) {
	jobsAccessControlSyncJobInterface = f
}

var pushProxyInterface func(*App) einterfaces.PushProxyInterface

func RegisterPushProxyInterface(f func(*App) einterfaces.PushProxyInterface) {
	pushProxyInterface = f
}

func (s *Server) initEnterprise() {
	RegisterCloudInterface(func(s *Server) einterfaces.CloudInterface {
		return NewFooCloudInterface()
	})
	if cloudInterface != nil {
		s.Cloud = cloudInterface(s)
	}
}

type FooCloudInterface struct {
	Products        []*model.Product
	Customer        *model.CloudCustomer
	Subscription    *model.Subscription
	Installation    *model.Installation
	AllowedIPRanges *model.AllowedIPRanges
}

func NewFooCloudInterface() *FooCloudInterface {
	ipRanges := &model.AllowedIPRanges{
		{
			CIDRBlock:   "0.0.0.0/0",
			Description: "All",
			Enabled:     true,
			OwnerID:     "bucod9hugtd49mqt6zsgtmp51y",
		},
	}
	return &FooCloudInterface{Products: []*model.Product{
		{
			ID:                "chat",
			Name:              "Chat",
			Description:       "Chat",
			PricePerSeat:      0,
			AddOns:            []*model.AddOn{},
			SKU:               "advanced",
			PriceID:           "deadbeef",
			Family:            "",
			RecurringInterval: "",
			BillingScheme:     "",
			CrossSellsTo:      "",
		},
	}, Customer: &model.CloudCustomer{
		CloudCustomerInfo: model.CloudCustomerInfo{
			Name:                  "Administrator Super",
			Email:                 "admin@example.com",
			ContactFirstName:      "Administrator",
			ContactLastName:       "Super",
			NumEmployees:          100000,
			CloudAltPaymentMethod: "",
		},
		ID:             "bucod9hugtd49mqt6zsgtmp51y",
		CreatorID:      "deadbeef",
		CreateAt:       1767526896148,
		BillingAddress: &model.Address{},
		CompanyAddress: &model.Address{},
		PaymentMethod:  &model.PaymentMethod{},
	}, Subscription: &model.Subscription{
		ID:                      "bucod9hugtd49mqt6zsgtmp51y",
		CustomerID:              "bucod9hugtd49mqt6zsgtmp51y",
		ProductID:               "",
		AddOns:                  []string{},
		StartAt:                 1767526896149,
		EndAt:                   4070880000000,
		CreateAt:                1767526896148,
		Seats:                   100000,
		Status:                  "",
		DNS:                     "",
		LastInvoice:             &model.Invoice{},
		UpcomingInvoice:         &model.Invoice{},
		IsFreeTrial:             "false",
		TrialEndAt:              4070880000000,
		DelinquentSince:         new(int64),
		OriginallyLicensedSeats: 0,
		ComplianceBlocked:       "",
		BillingType:             "",
		CancelAt:                new(int64),
		WillRenew:               "true",
		SimulatedCurrentTimeMs:  new(int64),
		IsCloudPreview:          false,
	}, Installation: &model.Installation{
		ID:              "",
		State:           "",
		AllowedIPRanges: ipRanges,
	}, AllowedIPRanges: ipRanges,
	}
}

func (c *FooCloudInterface) GetCloudProduct(userID string, productID string) (*model.Product, error) {
	return c.Products[0], nil
}
func (c *FooCloudInterface) GetCloudProducts(userID string, includeLegacyProducts bool) ([]*model.Product, error) {
	return c.Products, nil
}
func (c *FooCloudInterface) GetSelfHostedProducts(userID string) ([]*model.Product, error) {
	return c.Products, nil
}
func (c *FooCloudInterface) GetCloudLimits(userID string) (*model.ProductLimits, error) {
	teamsLimit := 9999
	return &model.ProductLimits{
		Files: &model.FilesLimits{
			TotalStorage: new(int64),
		},
		Messages: &model.MessagesLimits{
			History: new(int),
		},
		Teams: &model.TeamsLimits{
			Active: &teamsLimit,
		},
	}, nil
}
func (c *FooCloudInterface) GetCloudCustomer(userID string) (*model.CloudCustomer, error) {
	return &model.CloudCustomer{
		CloudCustomerInfo: model.CloudCustomerInfo{},
		ID:                userID,
		CreatorID:         "",
		CreateAt:          0,
		BillingAddress:    &model.Address{},
		CompanyAddress:    &model.Address{},
		PaymentMethod:     &model.PaymentMethod{},
	}, nil
}
func (c *FooCloudInterface) UpdateCloudCustomer(userID string, customerInfo *model.CloudCustomerInfo) (*model.CloudCustomer, error) {
	c.Customer.CloudCustomerInfo = *customerInfo
	return c.Customer, nil
}
func (c *FooCloudInterface) UpdateCloudCustomerAddress(userID string, address *model.Address) (*model.CloudCustomer, error) {
	c.Customer.BillingAddress = address
	return c.Customer, nil
}
func (c *FooCloudInterface) GetSubscription(userID string) (*model.Subscription, error) {
	return c.Subscription, nil
}
func (c *FooCloudInterface) GetInvoicesForSubscription(userID string) ([]*model.Invoice, error) {
	return []*model.Invoice{}, nil
}
func (c *FooCloudInterface) GetInvoicePDF(userID, invoiceID string) ([]byte, string, error) {
	return []byte{}, "invoice", nil
}
func (c *FooCloudInterface) ChangeSubscription(userID, subscriptionID string, subscriptionChange *model.SubscriptionChange) (*model.Subscription, error) {
	c.Subscription.ProductID = subscriptionChange.ProductID
	c.Subscription.Seats = subscriptionChange.Seats
	c.UpdateCloudCustomer(userID, subscriptionChange.Customer)
	return c.Subscription, nil
}
func (c *FooCloudInterface) ValidateBusinessEmail(userID, email string) error {
	return nil
}
func (c *FooCloudInterface) InvalidateCaches() error {
	return nil
}
func (c *FooCloudInterface) CreateOrUpdateSubscriptionHistoryEvent(userID string, userCount int) (*model.SubscriptionHistory, error) {
	return &model.SubscriptionHistory{}, nil
}
func (c *FooCloudInterface) HandleLicenseChange() error {
	return nil
}
func (c *FooCloudInterface) CheckCWSConnection(userId string) error {
	return nil
}
func (c *FooCloudInterface) SubscribeToNewsletter(userID string, req *model.SubscribeNewsletterRequest) error {
	return nil
}
func (c *FooCloudInterface) ApplyIPFilters(userID string, ranges *model.AllowedIPRanges) (*model.AllowedIPRanges, error) {
	c.AllowedIPRanges = ranges
	return ranges, nil
}
func (c *FooCloudInterface) GetIPFilters(userID string) (*model.AllowedIPRanges, error) {
	return c.AllowedIPRanges, nil
}
func (c *FooCloudInterface) GetInstallation(userID string) (*model.Installation, error) {
	return c.Installation, nil
}
func (c *FooCloudInterface) RemoveAuditLoggingCert(userID string) error {
	return nil
}
func (c *FooCloudInterface) CreateAuditLoggingCert(userID string, fileData *multipart.FileHeader) error {
	return nil
}
