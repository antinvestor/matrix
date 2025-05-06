// Copyright 2022 The Global.org Foundation C.I.C.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tables

import (
	"context"
	"encoding/json"
	"time"

	"golang.org/x/oauth2"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/userapi/api"

	clientapi "github.com/antinvestor/matrix/clientapi/api"
	"github.com/antinvestor/matrix/clientapi/auth/authtypes"
	"github.com/antinvestor/matrix/userapi/types"
)

type RegistrationTokensTable interface {
	RegistrationTokenExists(ctx context.Context, token string) (bool, error)
	InsertRegistrationToken(ctx context.Context, registrationToken *clientapi.RegistrationToken) (bool, error)
	ListRegistrationTokens(ctx context.Context, returnAll bool, valid bool) ([]clientapi.RegistrationToken, error)
	GetRegistrationToken(ctx context.Context, tokenString string) (*clientapi.RegistrationToken, error)
	DeleteRegistrationToken(ctx context.Context, tokenString string) error
	UpdateRegistrationToken(ctx context.Context, tokenString string, newAttributes map[string]interface{}) (*clientapi.RegistrationToken, error)
}

type AccountDataTable interface {
	InsertAccountData(ctx context.Context, localpart string, serverName spec.ServerName, roomID, dataType string, content json.RawMessage) error
	SelectAccountData(ctx context.Context, localpart string, serverName spec.ServerName) (map[string]json.RawMessage, map[string]map[string]json.RawMessage, error)
	SelectAccountDataByType(ctx context.Context, localpart string, serverName spec.ServerName, roomID, dataType string) (data json.RawMessage, err error)
}

type AccountsTable interface {
	InsertAccount(ctx context.Context, localpart string, serverName spec.ServerName, hash, appserviceID string, accountType api.AccountType) (*api.Account, error)
	UpdatePassword(ctx context.Context, localpart string, serverName spec.ServerName, passwordHash string) error
	DeactivateAccount(ctx context.Context, localpart string, serverName spec.ServerName) error
	SelectPasswordHash(ctx context.Context, localpart string, serverName spec.ServerName) (string, error)
	SelectAccountByLocalpart(ctx context.Context, localpart string, serverName spec.ServerName) (*api.Account, error)
	SelectNewNumericLocalpart(ctx context.Context, serverName spec.ServerName) (int64, error)
}

type DevicesTable interface {
	InsertDevice(ctx context.Context, id, localpart string, serverName spec.ServerName, accessToken string, extraData *oauth2.Token, displayName *string, ipAddr, userAgent string) (*api.Device, error)
	InsertDeviceWithSessionID(ctx context.Context, id, localpart string, serverName spec.ServerName, accessToken string, extraData *oauth2.Token, displayName *string, ipAddr, userAgent string, sessionID int64) (*api.Device, error)
	DeleteDevice(ctx context.Context, id, localpart string, serverName spec.ServerName) error
	DeleteDevices(ctx context.Context, localpart string, serverName spec.ServerName, devices []string) error
	DeleteDevicesByLocalpart(ctx context.Context, localpart string, serverName spec.ServerName, exceptDeviceID string) error
	UpdateDeviceName(ctx context.Context, localpart string, serverName spec.ServerName, deviceID string, displayName *string) error
	SelectDeviceByToken(ctx context.Context, accessToken string) (*api.Device, error)
	SelectDeviceByID(ctx context.Context, localpart string, serverName spec.ServerName, deviceID string) (*api.Device, error)
	SelectDevicesByLocalpart(ctx context.Context, localpart string, serverName spec.ServerName, exceptDeviceID string) ([]api.Device, error)
	SelectDevicesByID(ctx context.Context, deviceIDs []string) ([]api.Device, error)
	UpdateDeviceLastSeen(ctx context.Context, localpart string, serverName spec.ServerName, deviceID, ipAddr, userAgent string) error
}

type KeyBackupTable interface {
	CountKeys(ctx context.Context, userID, version string) (count int64, err error)
	InsertBackupKey(ctx context.Context, userID, version string, key api.InternalKeyBackupSession) (err error)
	UpdateBackupKey(ctx context.Context, userID, version string, key api.InternalKeyBackupSession) (err error)
	SelectKeys(ctx context.Context, userID, version string) (map[string]map[string]api.KeyBackupSession, error)
	SelectKeysByRoomID(ctx context.Context, userID, version, roomID string) (map[string]map[string]api.KeyBackupSession, error)
	SelectKeysByRoomIDAndSessionID(ctx context.Context, userID, version, roomID, sessionID string) (map[string]map[string]api.KeyBackupSession, error)
}

type KeyBackupVersionTable interface {
	InsertKeyBackup(ctx context.Context, userID, algorithm string, authData json.RawMessage, etag string) (version string, err error)
	UpdateKeyBackupAuthData(ctx context.Context, userID, version string, authData json.RawMessage) error
	UpdateKeyBackupETag(ctx context.Context, userID, version, etag string) error
	DeleteKeyBackup(ctx context.Context, userID, version string) (bool, error)
	SelectKeyBackup(ctx context.Context, userID, version string) (versionResult, algorithm string, authData json.RawMessage, etag string, deleted bool, err error)
}

type LoginTokenTable interface {
	InsertLoginToken(ctx context.Context, metadata *api.LoginTokenMetadata, data *api.LoginTokenData) error
	DeleteLoginToken(ctx context.Context, token string) error
	SelectLoginToken(ctx context.Context, token string) (*api.LoginTokenData, error)
}

type OpenIDTable interface {
	InsertOpenIDToken(ctx context.Context, token string, userID string, expiresAt int64) error
	SelectOpenIDToken(ctx context.Context, token string) (*api.OpenIDTokenAttributes, error)
	DeleteOpenIDToken(ctx context.Context, token string) error
}

type ProfileTable interface {
	InsertProfile(ctx context.Context, localpart string, serverName spec.ServerName) error
	SelectProfileByLocalpart(ctx context.Context, localpart string, serverName spec.ServerName) (*authtypes.Profile, error)
	SetAvatarURL(ctx context.Context, localpart string, serverName spec.ServerName, avatarURL string) (*authtypes.Profile, bool, error)
	SetDisplayName(ctx context.Context, localpart string, serverName spec.ServerName, displayName string) (*authtypes.Profile, bool, error)
	SelectProfilesBySearch(ctx context.Context, localpart, searchString string, limit int) ([]authtypes.Profile, error)
}

type ThreePIDTable interface {
	SelectLocalpartForThreePID(ctx context.Context, threepid string, medium string) (localpart string, serverName spec.ServerName, err error)
	SelectThreePIDsForLocalpart(ctx context.Context, localpart string, serverName spec.ServerName) (threepids []authtypes.ThreePID, err error)
	InsertThreePID(ctx context.Context, threepid, medium, localpart string, serverName spec.ServerName) (err error)
	DeleteThreePID(ctx context.Context, threepid string, medium string) (err error)
}

type PusherTable interface {
	InsertPusher(ctx context.Context, session_id int64, pushkey string, pushkeyTS int64, kind api.PusherKind, appid, appdisplayname, devicedisplayname, profiletag, lang, data, localpart string, serverName spec.ServerName) error
	SelectPushers(ctx context.Context, localpart string, serverName spec.ServerName) ([]api.Pusher, error)
	DeletePusher(ctx context.Context, appid, pushkey, localpart string, serverName spec.ServerName) error
	DeletePushers(ctx context.Context, appid, pushkey string) error
}

type NotificationTable interface {
	Clean(ctx context.Context) error
	Insert(ctx context.Context, localpart string, serverName spec.ServerName, eventID string, pos uint64, highlight bool, n *api.Notification) error
	DeleteUpTo(ctx context.Context, localpart string, serverName spec.ServerName, roomID string, pos uint64) (affected bool, _ error)
	UpdateRead(ctx context.Context, localpart string, serverName spec.ServerName, roomID string, pos uint64, v bool) (affected bool, _ error)
	Select(ctx context.Context, localpart string, serverName spec.ServerName, fromID int64, limit int, filter NotificationFilter) ([]*api.Notification, int64, error)
	SelectCount(ctx context.Context, localpart string, serverName spec.ServerName, filter NotificationFilter) (int64, error)
	SelectRoomCounts(ctx context.Context, localpart string, serverName spec.ServerName, roomID string) (total int64, highlight int64, _ error)
}

type StatsTable interface {
	// StatsTable defines the interface for statistics-related DB operations, without transactions.
	UserStatistics(ctx context.Context) (*types.UserStatistics, *types.DatabaseEngine, error)
	DailyRoomsMessages(ctx context.Context, serverName spec.ServerName) (msgStats types.MessageStats, activeRooms, activeE2EERooms int64, err error)
	UpdateUserDailyVisits(ctx context.Context, startTime, lastUpdate time.Time) error
	UpsertDailyStats(ctx context.Context, serverName spec.ServerName, stats types.MessageStats, activeRooms, activeE2EERooms int64) error
}

type NotificationFilter uint32

const (
	// HighlightNotifications returns notifications that had a
	// "highlight" tweak assigned to them from evaluating push rules.
	HighlightNotifications NotificationFilter = 1 << iota

	// NonHighlightNotifications returns notifications that don't
	// match HighlightNotifications.
	NonHighlightNotifications

	// NoNotifications is a filter to exclude all types of
	// notifications. It's useful as a zero value, but isn't likely to
	// be used in a call to Notifications.Select*.
	NoNotifications NotificationFilter = 0

	// AllNotifications is a filter to include all types of
	// notifications in Notifications.Select*. Note that PostgreSQL
	// balks if this doesn't fit in INTEGER, even though we use
	// uint32.
	AllNotifications NotificationFilter = (1 << 31) - 1
)

// OneTimeKeys defines the interface for one-time keys DB operations, without transactions.
type OneTimeKeys interface {
	SelectOneTimeKeys(ctx context.Context, userID, deviceID string, keyIDsWithAlgorithms []string) (map[string]json.RawMessage, error)
	CountOneTimeKeys(ctx context.Context, userID, deviceID string) (*api.OneTimeKeysCount, error)
	InsertOneTimeKeys(ctx context.Context, keys api.OneTimeKeys) (*api.OneTimeKeysCount, error)
	SelectAndDeleteOneTimeKey(ctx context.Context, userID, deviceID, algorithm string) (map[string]json.RawMessage, error)
	DeleteOneTimeKeys(ctx context.Context, userID, deviceID string) error
}

// DeviceKeys defines the interface for device keys DB operations, without transactions.
type DeviceKeys interface {
	SelectDeviceKeysJSON(ctx context.Context, keys []api.DeviceMessage) error
	InsertDeviceKeys(ctx context.Context, keys []api.DeviceMessage) error
	SelectMaxStreamIDForUser(ctx context.Context, userID string) (streamID int64, err error)
	CountStreamIDsForUser(ctx context.Context, userID string, streamIDs []int64) (int, error)
	SelectBatchDeviceKeys(ctx context.Context, userID string, deviceIDs []string, includeEmpty bool) ([]api.DeviceMessage, error)
	DeleteDeviceKeys(ctx context.Context, userID, deviceID string) error
	DeleteAllDeviceKeys(ctx context.Context, userID string) error
}

type KeyChanges interface {
	InsertKeyChange(ctx context.Context, userID string) (int64, error)
	// SelectKeyChanges returns the set (de-duplicated) of users who have changed their keys between the two offsets.
	// Results are exclusive of fromOffset and inclusive of toOffset. A toOffset of types.OffsetNewest means no upper offset.
	SelectKeyChanges(ctx context.Context, fromOffset, toOffset int64) (userIDs []string, latestOffset int64, err error)
}

// StaleDeviceLists defines the interface for stale device list DB operations, without transactions.
type StaleDeviceLists interface {
	InsertStaleDeviceList(ctx context.Context, userID string, isStale bool) error
	SelectUserIDsWithStaleDeviceLists(ctx context.Context, domains []spec.ServerName) ([]string, error)
	DeleteStaleDeviceLists(ctx context.Context, userIDs []string) error
}

// CrossSigningKeys defines the interface for cross-signing keys DB operations, without transactions.
type CrossSigningKeys interface {
	SelectCrossSigningKeysForUser(ctx context.Context, userID string) (r types.CrossSigningKeyMap, err error)
	UpsertCrossSigningKeysForUser(ctx context.Context, userID string, keyType fclient.CrossSigningKeyPurpose, keyData spec.Base64Bytes) error
}

// CrossSigningSigs defines the interface for cross-signing signatures DB operations, without transactions.
type CrossSigningSigs interface {
	SelectCrossSigningSigsForTarget(ctx context.Context, originUserID, targetUserID string, targetKeyID gomatrixserverlib.KeyID) (r types.CrossSigningSigMap, err error)
	UpsertCrossSigningSigsForTarget(ctx context.Context, originUserID string, originKeyID gomatrixserverlib.KeyID, targetUserID string, targetKeyID gomatrixserverlib.KeyID, signature spec.Base64Bytes) error
	DeleteCrossSigningSigsForTarget(ctx context.Context, targetUserID string, targetKeyID gomatrixserverlib.KeyID) error
}
