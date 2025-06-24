package config

import (
	"fmt"

	"github.com/antinvestor/matrix/setup/constants"
	"golang.org/x/crypto/bcrypt"
)

type UserAPI struct {
	Global *Global `yaml:"-"`

	// The cost when hashing passwords.
	BCryptCost int `yaml:"bcrypt_cost"`

	// The length of time an OpenID token is condidered valid in milliseconds
	OpenIDTokenLifetimeMS int64 `yaml:"openid_token_lifetime_ms"`

	// Disable TLS validation on HTTPS calls to push gatways. NOT RECOMMENDED!
	PushGatewayDisableTLSValidation bool `yaml:"push_gateway_disable_tls_validation"`

	// The Account database stores the login details and account information
	// for local users. It is accessed by the UserAPI.
	AccountDatabase DatabaseOptions `yaml:"account_database,omitempty"`

	// Users who register on this homeserver will automatically
	// be joined to the rooms listed under this option.
	AutoJoinRooms []string `yaml:"auto_join_rooms"`

	// The number of workers to start for the DeviceListUpdater. Defaults to 8.
	// This only needs updating if the "InputDeviceListUpdate" stream keeps growing indefinitely.
	WorkerCount int `yaml:"worker_count"`

	Queues UserAPIQueues `yaml:"queues"`
}

const DefaultOpenIDTokenLifetimeMS = 3600000 // 60 minutes

func (c *UserAPI) Defaults(opts DefaultOpts) {
	c.BCryptCost = bcrypt.DefaultCost
	c.OpenIDTokenLifetimeMS = DefaultOpenIDTokenLifetimeMS
	c.WorkerCount = 8
	c.AccountDatabase.Reference = "UserAPI"
	c.AccountDatabase.Prefix = opts.RandomnessPrefix
	c.AccountDatabase.DatabaseURI = opts.DSDatabaseConn
	c.Queues.Defaults(opts)
}

func (c *UserAPI) Verify(configErrs *Errors) {
	checkPositive(configErrs, "user_api.openid_token_lifetime_ms", c.OpenIDTokenLifetimeMS)
	if c.AccountDatabase.DatabaseURI == "" {
		checkNotEmpty(configErrs, "user_api.account_database.database_uri", string(c.AccountDatabase.DatabaseURI))
	}
}

type UserAPIQueues struct {
	// durable - UserAPIReceiptConsumer
	OutputReceiptEvent QueueOptions `yaml:"output_receipt_event"`

	// durable - KeyServerInputDeviceListConsumer
	InputDeviceListUpdate QueueOptions `yaml:"input_device_list_update"`

	// durable - KeyServerSigningKeyConsumer
	InputSigningKeyUpdate QueueOptions `yaml:"input_signing_key_update"`

	// durable - UserAPIRoomServerConsumer
	OutputRoomEvent QueueOptions `yaml:"output_room_event"`
}

func (q *UserAPIQueues) Defaults(opts DefaultOpts) {
	q.OutputReceiptEvent = opts.defaultQ(constants.OutputReceiptEvent)
	q.InputDeviceListUpdate = opts.defaultQ(constants.InputDeviceListUpdate)
	q.InputSigningKeyUpdate = opts.defaultQ(constants.InputSigningKeyUpdate)

	q.OutputRoomEvent = opts.defaultQ(constants.OutputRoomEvent,
		KVOpt{K: "stream_retention", V: "interest"},
		KVOpt{K: "stream_subjects", V: fmt.Sprintf("%s.*", constants.OutputRoomEvent)},
		KVOpt{K: "consumer_filter_subject", V: fmt.Sprintf("%s.*", constants.OutputRoomEvent)},
		KVOpt{K: "consumer_durable_name", V: "CnsDurable_UserAPIOutputRoomEvent"},
		KVOpt{K: "consumer_headers_only", V: "true"},
		KVOpt{K: constants.QueueHeaderToExtendSubject, V: constants.RoomID})
	q.OutputRoomEvent.QReference = fmt.Sprintf("UserAPI_%s", q.OutputRoomEvent.QReference)

}

func (q *UserAPIQueues) Verify(configErrs *Errors) {
	checkNotEmpty(configErrs, "user_api.queues.output_receipt_event", string(q.OutputReceiptEvent.DS))
	checkNotEmpty(configErrs, "user_api.queues.input_device_list_update", string(q.InputDeviceListUpdate.DS))
	checkNotEmpty(configErrs, "user_api.queues.input_signing_key_update", string(q.InputSigningKeyUpdate.DS))
	checkNotEmpty(configErrs, "user_api.queues.output_room_event", string(q.OutputRoomEvent.DS))
}
