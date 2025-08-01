package storage_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/clientapi/auth/authtypes"
	"github.com/antinvestor/matrix/internal/pushrules"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/syncapi/synctypes"
	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/test/testrig"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/storage"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"github.com/antinvestor/matrix/userapi/types"
	"github.com/pitabwire/frame"
	"github.com/pitabwire/util"
	"github.com/stretchr/testify/assert"
	"golang.org/x/crypto/bcrypt"
)

const loginTokenLifetime = time.Minute

var (
	openIDLifetimeMS = time.Minute.Milliseconds()
)

func mustCreateUserDatabase(ctx context.Context, svc *frame.Service, t *testing.T, _ test.DependancyOption) storage.UserDatabase {

	cm := sqlutil.NewConnectionManager(svc)

	db, err := storage.NewUserDatabase(ctx, nil, nil, cm, "localhost", bcrypt.MinCost, openIDLifetimeMS, loginTokenLifetime, "_server")
	if err != nil {
		t.Fatalf("NewUserDatabase returned %s", err)
	}
	return db
}

// Tests storing and getting account data
func Test_AccountData(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db := mustCreateUserDatabase(ctx, svc, t, testOpts)

		alice := test.NewUser(t)
		localpart, domain, err := gomatrixserverlib.SplitID('@', alice.ID)
		assert.NoError(t, err)

		room := test.NewRoom(t, alice)
		events := room.Events()

		contentRoom := json.RawMessage(fmt.Sprintf(`{"event_id":"%s"}`, events[len(events)-1].EventID()))
		err = db.SaveAccountData(ctx, localpart, domain, room.ID, "m.fully_read", contentRoom)
		assert.NoError(t, err, "unable to save account data")

		contentGlobal := json.RawMessage(fmt.Sprintf(`{"recent_rooms":["%s"]}`, room.ID))
		err = db.SaveAccountData(ctx, localpart, domain, "", "im.vector.setting.breadcrumbs", contentGlobal)
		assert.NoError(t, err, "unable to save account data")

		accountData, err := db.GetAccountDataByType(ctx, localpart, domain, room.ID, "m.fully_read")
		assert.NoError(t, err, "unable to get account data by type")
		assert.Equal(t, contentRoom, accountData)

		globalData, roomData, err := db.GetAccountData(ctx, localpart, domain)
		assert.NoError(t, err)
		assert.Equal(t, contentRoom, roomData[room.ID]["m.fully_read"])
		assert.Equal(t, contentGlobal, globalData["im.vector.setting.breadcrumbs"])
	})
}

// Tests the creation of accounts
func Test_Accounts(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db := mustCreateUserDatabase(ctx, svc, t, testOpts)

		alice := test.NewUser(t)
		aliceLocalpart, aliceDomain, err := gomatrixserverlib.SplitID('@', alice.ID)
		assert.NoError(t, err)

		accAlice, err := db.CreateAccount(ctx, aliceLocalpart, aliceDomain, "testing", "", api.AccountTypeAdmin)
		assert.NoError(t, err, "failed to create account")
		// verify the newly create account is the same as returned by CreateAccount
		var accGet *api.Account
		accGet, err = db.GetAccountByPassword(ctx, aliceLocalpart, aliceDomain, "testing")
		assert.NoError(t, err, "failed to get account by password")
		assert.Equal(t, accAlice, accGet)
		accGet, err = db.GetAccountByLocalpart(ctx, aliceLocalpart, aliceDomain)
		assert.NoError(t, err, "failed to get account by localpart")
		assert.Equal(t, accAlice, accGet)

		// check account availability
		available, err := db.CheckAccountAvailability(ctx, aliceLocalpart, aliceDomain)
		assert.NoError(t, err, "failed to checkout account availability")
		assert.Equal(t, false, available)

		available, err = db.CheckAccountAvailability(ctx, "unusedname", aliceDomain)
		assert.NoError(t, err, "failed to checkout account availability")
		assert.Equal(t, true, available)

		// get guest account numeric aliceLocalpart
		first, err := db.GetNewNumericLocalpart(ctx, aliceDomain)
		assert.NoError(t, err, "failed to get new numeric localpart")
		// Create a new account to verify the numeric localpart is updated
		_, err = db.CreateAccount(ctx, "", aliceDomain, "testing", "", api.AccountTypeGuest)
		assert.NoError(t, err, "failed to create account")
		second, err := db.GetNewNumericLocalpart(ctx, aliceDomain)
		assert.NoError(t, err)
		assert.Greater(t, second, first)

		// update password for alice
		err = db.SetPassword(ctx, aliceLocalpart, aliceDomain, "newPassword")
		assert.NoError(t, err, "failed to update password")
		accGet, err = db.GetAccountByPassword(ctx, aliceLocalpart, aliceDomain, "newPassword")
		assert.NoError(t, err, "failed to get account by new password")
		assert.Equal(t, accAlice, accGet)

		// deactivate account
		err = db.DeactivateAccount(ctx, aliceLocalpart, aliceDomain)
		assert.NoError(t, err, "failed to deactivate account")
		// This should fail now, as the account is deactivated
		_, err = db.GetAccountByPassword(ctx, aliceLocalpart, aliceDomain, "newPassword")
		assert.Errorf(t, err, "expected an error, got none")

		_, err = db.GetAccountByLocalpart(ctx, "unusename", aliceDomain)
		assert.Errorf(t, err, "expected an error for non existent localpart")

		// create an empty localpart; this should never happen, but is required to test getting a numeric localpart
		// if there's already a user without a localpart in the database
		_, err = db.CreateAccount(ctx, "", aliceDomain, "", "", api.AccountTypeUser)
		assert.NoError(t, err)

		// test getting a numeric localpart, with an existing user without a localpart
		_, err = db.CreateAccount(ctx, "", aliceDomain, "", "", api.AccountTypeGuest)
		assert.NoError(t, err)

		// Create a user with a high numeric localpart, out of range for the Postgres integer (2147483647) type
		_, err = db.CreateAccount(ctx, "2147483650", aliceDomain, "", "", api.AccountTypeUser)
		assert.NoError(t, err)

		// Now try to create a new guest user
		_, err = db.CreateAccount(ctx, "", aliceDomain, "", "", api.AccountTypeGuest)
		assert.NoError(t, err)
	})
}

func Test_Devices(t *testing.T) {
	alice := test.NewUser(t)
	localpart, domain, err := gomatrixserverlib.SplitID('@', alice.ID)
	assert.NoError(t, err)
	deviceID := util.RandomString(8)
	accessToken := util.RandomString(16)

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db := mustCreateUserDatabase(ctx, svc, t, testOpts)

		deviceWithID, err := db.CreateDevice(ctx, localpart, domain, &deviceID, accessToken, nil, nil, "", "")
		assert.NoError(t, err, "unable to create deviceWithoutID")

		gotDevice, err := db.GetDeviceByID(ctx, localpart, domain, deviceID)
		assert.NoError(t, err, "unable to get device by id")
		assert.Equal(t, deviceWithID.ID, gotDevice.ID) // GetDeviceByID doesn't populate all fields

		_, gotDeviceAccessToken, err := db.GetDeviceByAccessToken(ctx, accessToken)
		assert.NoError(t, err, "unable to get device by access token")
		assert.Equal(t, deviceWithID.ID, gotDeviceAccessToken.ID) // GetDeviceByAccessToken doesn't populate all fields

		// create a device without existing device ID
		accessToken = util.RandomString(16)
		deviceWithoutID, err := db.CreateDevice(ctx, localpart, domain, nil, accessToken, nil, nil, "", "")
		assert.NoError(t, err, "unable to create deviceWithoutID")
		gotDeviceWithoutID, err := db.GetDeviceByID(ctx, localpart, domain, deviceWithoutID.ID)
		assert.NoError(t, err, "unable to get device by id")
		assert.Equal(t, deviceWithoutID.ID, gotDeviceWithoutID.ID) // GetDeviceByID doesn't populate all fields

		// Get devices
		devices, err := db.GetDevicesByLocalpart(ctx, localpart, domain)
		assert.NoError(t, err, "unable to get devices by localpart")
		assert.Equal(t, 2, len(devices))
		deviceIDs := make([]string, 0, len(devices))
		for _, dev := range devices {
			deviceIDs = append(deviceIDs, dev.ID)
		}

		devices2, err := db.GetDevicesByID(ctx, deviceIDs)
		assert.NoError(t, err, "unable to get devices by id")
		assert.ElementsMatch(t, devices, devices2)

		// Update device
		newName := "new display name"
		err = db.UpdateDevice(ctx, localpart, domain, deviceWithID.ID, &newName)
		assert.NoError(t, err, "unable to update device displayname")
		updatedAfterTimestamp := time.Now().Unix()
		err = db.UpdateDeviceLastSeen(ctx, localpart, domain, deviceWithID.ID, "127.0.0.1", "Element Web")
		assert.NoError(t, err, "unable to update device last seen")

		deviceWithID.DisplayName = newName
		deviceWithID.LastSeenIP = "127.0.0.1"
		gotDevice, err = db.GetDeviceByID(ctx, localpart, domain, deviceWithID.ID)
		assert.NoError(t, err, "unable to get device by id")
		assert.Equal(t, 2, len(devices))
		assert.Equal(t, deviceWithID.DisplayName, gotDevice.DisplayName)
		assert.Equal(t, deviceWithID.LastSeenIP, gotDevice.LastSeenIP)
		assert.Greater(t, gotDevice.LastSeenTS, updatedAfterTimestamp)

		// create one more device and remove the devices step by step
		newDeviceID := util.RandomString(16)
		accessToken = util.RandomString(16)
		_, err = db.CreateDevice(ctx, localpart, domain, &newDeviceID, accessToken, nil, nil, "", "")
		assert.NoError(t, err, "unable to create new device")

		devices, err = db.GetDevicesByLocalpart(ctx, localpart, domain)
		assert.NoError(t, err, "unable to get device by id")
		assert.Equal(t, 3, len(devices))

		err = db.RemoveDevices(ctx, localpart, domain, deviceIDs)
		assert.NoError(t, err, "unable to remove devices")
		devices, err = db.GetDevicesByLocalpart(ctx, localpart, domain)
		assert.NoError(t, err, "unable to get device by id")
		assert.Equal(t, 1, len(devices))

		deleted, err := db.RemoveAllDevices(ctx, localpart, domain, "")
		assert.NoError(t, err, "unable to remove all devices")
		assert.Equal(t, 1, len(deleted))
		assert.Equal(t, newDeviceID, deleted[0].ID)
	})
}

func Test_KeyBackup(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db := mustCreateUserDatabase(ctx, svc, t, testOpts)

		wantAuthData := json.RawMessage("my auth data")
		wantVersion, err := db.CreateKeyBackup(ctx, alice.ID, "dummyAlgo", wantAuthData)
		assert.NoError(t, err, "unable to create key backup")
		// get key backup by version
		gotVersion, gotAlgo, gotAuthData, _, _, err := db.GetKeyBackup(ctx, alice.ID, wantVersion)
		assert.NoError(t, err, "unable to get key backup")
		assert.Equal(t, wantVersion, gotVersion, "backup version mismatch")
		assert.Equal(t, "dummyAlgo", gotAlgo, "backup algorithm mismatch")
		assert.Equal(t, wantAuthData, gotAuthData, "backup auth data mismatch")

		// get any key backup
		gotVersion, gotAlgo, gotAuthData, _, _, err = db.GetKeyBackup(ctx, alice.ID, "")
		assert.NoError(t, err, "unable to get key backup")
		assert.Equal(t, wantVersion, gotVersion, "backup version mismatch")
		assert.Equal(t, "dummyAlgo", gotAlgo, "backup algorithm mismatch")
		assert.Equal(t, wantAuthData, gotAuthData, "backup auth data mismatch")

		err = db.UpdateKeyBackupAuthData(ctx, alice.ID, wantVersion, json.RawMessage("my updated auth data"))
		assert.NoError(t, err, "unable to update key backup auth data")

		uploads := []api.InternalKeyBackupSession{
			{
				KeyBackupSession: api.KeyBackupSession{
					IsVerified:  true,
					SessionData: wantAuthData,
				},
				RoomID:    room.ID,
				SessionID: "1",
			},
			{
				KeyBackupSession: api.KeyBackupSession{},
				RoomID:           room.ID,
				SessionID:        "2",
			},
		}
		count, _, err := db.UpsertBackupKeys(ctx, wantVersion, alice.ID, uploads)
		assert.NoError(t, err, "unable to upsert backup keys")
		assert.Equal(t, int64(len(uploads)), count, "unexpected backup count")

		// do it again to update a key
		uploads[1].IsVerified = true
		count, _, err = db.UpsertBackupKeys(ctx, wantVersion, alice.ID, uploads[1:])
		assert.NoError(t, err, "unable to upsert backup keys")
		assert.Equal(t, int64(len(uploads)), count, "unexpected backup count")

		// get backup keys by session id
		gotBackupKeys, err := db.GetBackupKeys(ctx, wantVersion, alice.ID, room.ID, "1")
		assert.NoError(t, err, "unable to get backup keys")
		assert.Equal(t, uploads[0].KeyBackupSession, gotBackupKeys[room.ID]["1"])

		// get backup keys by room id
		gotBackupKeys, err = db.GetBackupKeys(ctx, wantVersion, alice.ID, room.ID, "")
		assert.NoError(t, err, "unable to get backup keys")
		assert.Equal(t, uploads[0].KeyBackupSession, gotBackupKeys[room.ID]["1"])

		gotCount, err := db.CountBackupKeys(ctx, wantVersion, alice.ID)
		assert.NoError(t, err, "unable to get backup keys count")
		assert.Equal(t, count, gotCount, "unexpected backup count")

		// finally delete a key
		exists, err := db.DeleteKeyBackup(ctx, alice.ID, wantVersion)
		assert.NoError(t, err, "unable to delete key backup")
		assert.True(t, exists)

		// this key should not exist
		exists, err = db.DeleteKeyBackup(ctx, alice.ID, "3")
		assert.NoError(t, err, "unable to delete key backup")
		assert.False(t, exists)
	})
}

func Test_LoginToken(t *testing.T) {
	alice := test.NewUser(t)
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db := mustCreateUserDatabase(ctx, svc, t, testOpts)

		// create a new token
		wantLoginToken := &api.LoginTokenData{UserID: alice.ID}

		gotMetadata, err := db.CreateLoginToken(ctx, wantLoginToken)
		assert.NoError(t, err, "unable to create login token")
		assert.NotNil(t, gotMetadata)
		assert.Equal(t, time.Now().Add(loginTokenLifetime).Truncate(loginTokenLifetime), gotMetadata.Expiration.Truncate(loginTokenLifetime))

		// get the new token
		gotLoginToken, err := db.GetLoginTokenDataByToken(ctx, gotMetadata.Token)
		assert.NoError(t, err, "unable to get login token")
		assert.NotNil(t, gotLoginToken)
		assert.Equal(t, wantLoginToken, gotLoginToken, "unexpected login token")

		// remove the login token again
		err = db.RemoveLoginToken(ctx, gotMetadata.Token)
		assert.NoError(t, err, "unable to remove login token")

		// check if the token was actually deleted
		_, err = db.GetLoginTokenDataByToken(ctx, gotMetadata.Token)
		assert.Errorf(t, err, "expected an error, but got none")
	})
}

func Test_OpenID(t *testing.T) {
	alice := test.NewUser(t)
	token := util.RandomString(24)

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db := mustCreateUserDatabase(ctx, svc, t, testOpts)

		expiresAtMS := time.Now().UnixNano()/int64(time.Millisecond) + openIDLifetimeMS
		expires, err := db.CreateOpenIDToken(ctx, token, alice.ID)
		assert.NoError(t, err, "unable to create OpenID token")
		assert.InDelta(t, expiresAtMS, expires, 2) // 2ms leeway

		attributes, err := db.GetOpenIDTokenAttributes(ctx, token)
		assert.NoError(t, err, "unable to get OpenID token attributes")
		assert.Equal(t, alice.ID, attributes.UserID)
		assert.InDelta(t, expiresAtMS, attributes.ExpiresAtMS, 2) // 2ms leeway
	})
}

func Test_Profile(t *testing.T) {
	alice := test.NewUser(t)
	aliceLocalpart, aliceDomain, err := gomatrixserverlib.SplitID('@', alice.ID)
	assert.NoError(t, err)

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db := mustCreateUserDatabase(ctx, svc, t, testOpts)

		// create account, which also creates a profile
		_, err = db.CreateAccount(ctx, aliceLocalpart, aliceDomain, "testing", "", api.AccountTypeAdmin)
		assert.NoError(t, err, "failed to create account")

		gotProfile, err := db.GetProfileByLocalpart(ctx, aliceLocalpart, aliceDomain)
		assert.NoError(t, err, "unable to get profile by localpart")
		wantProfile := &authtypes.Profile{
			Localpart:  aliceLocalpart,
			ServerName: string(aliceDomain),
		}
		assert.Equal(t, wantProfile, gotProfile)

		// set avatar & displayname
		wantProfile.DisplayName = "Alice"
		gotProfile, changed, err := db.SetDisplayName(ctx, aliceLocalpart, aliceDomain, "Alice")
		assert.Equal(t, wantProfile, gotProfile)
		assert.NoError(t, err, "unable to set displayname")
		assert.True(t, changed)

		wantProfile.AvatarURL = "mxc://aliceAvatar"
		gotProfile, changed, err = db.SetAvatarURL(ctx, aliceLocalpart, aliceDomain, "mxc://aliceAvatar")
		assert.NoError(t, err, "unable to set avatar url")
		assert.Equal(t, wantProfile, gotProfile)
		assert.True(t, changed)

		// Setting the same avatar again doesn't change anything
		wantProfile.AvatarURL = "mxc://aliceAvatar"
		gotProfile, changed, err = db.SetAvatarURL(ctx, aliceLocalpart, aliceDomain, "mxc://aliceAvatar")
		assert.NoError(t, err, "unable to set avatar url")
		assert.Equal(t, wantProfile, gotProfile)
		assert.False(t, changed)

		// search profiles
		searchRes, err := db.SearchProfiles(ctx, "Alice", "Alice", 2)
		assert.NoError(t, err, "unable to search profiles")
		assert.Equal(t, 1, len(searchRes))
		assert.Equal(t, *wantProfile, searchRes[0])
	})
}

func Test_Pusher(t *testing.T) {
	alice := test.NewUser(t)
	aliceLocalpart, aliceDomain, err := gomatrixserverlib.SplitID('@', alice.ID)
	assert.NoError(t, err)

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db := mustCreateUserDatabase(ctx, svc, t, testOpts)

		appID := util.RandomString(8)
		var pushKeys []string
		var gotPushers []api.Pusher
		for i := 0; i < 2; i++ {
			pushKey := util.RandomString(8)

			wantPusher := api.Pusher{
				PushKey:           pushKey,
				Kind:              api.HTTPKind,
				AppID:             appID,
				AppDisplayName:    util.RandomString(8),
				DeviceDisplayName: util.RandomString(8),
				ProfileTag:        util.RandomString(8),
				Language:          util.RandomString(2),
			}
			err = db.UpsertPusher(ctx, wantPusher, aliceLocalpart, aliceDomain)
			assert.NoError(t, err, "unable to upsert pusher")

			// check it was actually persisted
			gotPushers, err = db.GetPushers(ctx, aliceLocalpart, aliceDomain)
			assert.NoError(t, err, "unable to get pushers")
			assert.Equal(t, i+1, len(gotPushers))
			assert.Equal(t, wantPusher, gotPushers[i])
			pushKeys = append(pushKeys, pushKey)
		}

		// remove single pusher
		err = db.RemovePusher(ctx, appID, pushKeys[0], aliceLocalpart, aliceDomain)
		assert.NoError(t, err, "unable to remove pusher")
		gotPushers, err := db.GetPushers(ctx, aliceLocalpart, aliceDomain)
		assert.NoError(t, err, "unable to get pushers")
		assert.Equal(t, 1, len(gotPushers))

		// remove last pusher
		err = db.RemovePushers(ctx, appID, pushKeys[1])
		assert.NoError(t, err, "unable to remove pusher")
		gotPushers, err = db.GetPushers(ctx, aliceLocalpart, aliceDomain)
		assert.NoError(t, err, "unable to get pushers")
		assert.Equal(t, 0, len(gotPushers))
	})
}

func Test_ThreePID(t *testing.T) {
	alice := test.NewUser(t)
	aliceLocalpart, aliceDomain, err := gomatrixserverlib.SplitID('@', alice.ID)
	assert.NoError(t, err)

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db := mustCreateUserDatabase(ctx, svc, t, testOpts)

		threePID := util.RandomString(8)
		medium := util.RandomString(8)
		err = db.SaveThreePIDAssociation(ctx, threePID, aliceLocalpart, aliceDomain, medium)
		assert.NoError(t, err, "unable to save threepid association")

		// get the stored threepid
		gotLocalpart, gotDomain, err := db.GetLocalpartForThreePID(ctx, threePID, medium)
		assert.NoError(t, err, "unable to get localpart for threepid")
		assert.Equal(t, aliceLocalpart, gotLocalpart)
		assert.Equal(t, aliceDomain, gotDomain)

		threepids, err := db.GetThreePIDsForLocalpart(ctx, aliceLocalpart, aliceDomain)
		assert.NoError(t, err, "unable to get threepids for localpart")
		assert.Equal(t, 1, len(threepids))
		assert.Equal(t, authtypes.ThreePID{
			Address: threePID,
			Medium:  medium,
		}, threepids[0])

		// remove threepid association
		err = db.RemoveThreePIDAssociation(ctx, threePID, medium)
		assert.NoError(t, err, "unexpected error")

		// verify it was deleted
		threepids, err = db.GetThreePIDsForLocalpart(ctx, aliceLocalpart, aliceDomain)
		assert.NoError(t, err, "unable to get threepids for localpart")
		assert.Equal(t, 0, len(threepids))
	})
}

func Test_Notification(t *testing.T) {
	alice := test.NewUser(t)
	aliceLocalpart, aliceDomain, err := gomatrixserverlib.SplitID('@', alice.ID)
	assert.NoError(t, err)
	room := test.NewRoom(t, alice)
	room2 := test.NewRoom(t, alice)
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db := mustCreateUserDatabase(ctx, svc, t, testOpts)

		// generate some dummy notifications
		for i := 0; i < 10; i++ {
			eventID := util.RandomString(16)
			roomID := room.ID
			ts := time.Now()
			if i > 5 {
				roomID = room2.ID
				// create some old notifications to test DeleteOldNotifications
				ts = ts.AddDate(0, -2, 0)
			}
			notification := &api.Notification{
				Actions: []*pushrules.Action{
					{},
				},
				Event: synctypes.ClientEvent{
					Content: json.RawMessage("{}"),
				},
				Read:   false,
				RoomID: roomID,
				TS:     spec.AsTimestamp(ts),
			}
			err = db.InsertNotification(ctx, aliceLocalpart, aliceDomain, eventID, uint64(i+1), nil, notification)
			assert.NoError(t, err, "unable to insert notification")
		}

		// get notifications
		count, err := db.GetNotificationCount(ctx, aliceLocalpart, aliceDomain, tables.AllNotifications)
		assert.NoError(t, err, "unable to get notification count")
		assert.Equal(t, int64(10), count)
		notifs, count, err := db.GetNotifications(ctx, aliceLocalpart, aliceDomain, 0, 15, tables.AllNotifications)
		assert.NoError(t, err, "unable to get notifications")
		assert.Equal(t, int64(10), count)
		assert.Equal(t, 10, len(notifs))
		// ... for a specific room
		total, _, err := db.GetRoomNotificationCounts(ctx, aliceLocalpart, aliceDomain, room2.ID)
		assert.NoError(t, err, "unable to get notifications for room")
		assert.Equal(t, int64(4), total)

		// mark notification as read
		affected, err := db.SetNotificationsRead(ctx, aliceLocalpart, aliceDomain, room2.ID, 7, true)
		assert.NoError(t, err, "unable to set notifications read")
		assert.True(t, affected)

		// this should delete 2 notifications
		affected, err = db.DeleteNotificationsUpTo(ctx, aliceLocalpart, aliceDomain, room2.ID, 8)
		assert.NoError(t, err, "unable to set notifications read")
		assert.True(t, affected)

		total, _, err = db.GetRoomNotificationCounts(ctx, aliceLocalpart, aliceDomain, room2.ID)
		assert.NoError(t, err, "unable to get notifications for room")
		assert.Equal(t, int64(2), total)

		// delete old notifications
		err = db.DeleteOldNotifications(ctx)
		assert.NoError(t, err)

		// this should now return 0 notifications
		total, _, err = db.GetRoomNotificationCounts(ctx, aliceLocalpart, aliceDomain, room2.ID)
		assert.NoError(t, err, "unable to get notifications for room")
		assert.Equal(t, int64(0), total)
	})
}

func mustCreateKeyDatabase(ctx context.Context, svc *frame.Service, t *testing.T, _ test.DependancyOption) storage.KeyDatabase {

	cm := sqlutil.NewConnectionManager(svc)
	db, err := storage.NewKeyDatabase(ctx, cm)
	if err != nil {
		t.Fatalf("failed to create new database: %v", err)
	}
	return db
}

func MustNotError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		return
	}
	t.Fatalf("operation failed: %s", err)
}

func TestKeyChanges(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db := mustCreateKeyDatabase(ctx, svc, t, testOpts)

		_, err := db.StoreKeyChange(ctx, "@alice:localhost")
		MustNotError(t, err)
		deviceChangeIDB, err := db.StoreKeyChange(ctx, "@bob:localhost")
		MustNotError(t, err)
		deviceChangeIDC, err := db.StoreKeyChange(ctx, "@charlie:localhost")
		MustNotError(t, err)
		userIDs, latest, err := db.KeyChanges(ctx, deviceChangeIDB, types.OffsetNewest)
		if err != nil {
			t.Fatalf("Failed to KeyChanges: %s", err)
		}
		if latest != deviceChangeIDC {
			t.Fatalf("KeyChanges: got latest=%d want %d", latest, deviceChangeIDC)
		}
		if !reflect.DeepEqual(userIDs, []string{"@charlie:localhost"}) {
			t.Fatalf("KeyChanges: wrong user_ids: %v", userIDs)
		}
	})
}

func TestKeyChangesNoDupes(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db := mustCreateKeyDatabase(ctx, svc, t, testOpts)

		deviceChangeIDA, err := db.StoreKeyChange(ctx, "@alice:localhost")
		MustNotError(t, err)
		deviceChangeIDB, err := db.StoreKeyChange(ctx, "@alice:localhost")
		MustNotError(t, err)
		if deviceChangeIDA == deviceChangeIDB {
			t.Fatalf("Expected change ID to be different even when inserting key change for the same user, got %d for both changes", deviceChangeIDA)
		}
		deviceChangeID, err := db.StoreKeyChange(ctx, "@alice:localhost")
		MustNotError(t, err)
		userIDs, latest, err := db.KeyChanges(ctx, 0, types.OffsetNewest)
		if err != nil {
			t.Fatalf("Failed to KeyChanges: %s", err)
		}
		if latest != deviceChangeID {
			t.Fatalf("KeyChanges: got latest=%d want %d", latest, deviceChangeID)
		}
		if !reflect.DeepEqual(userIDs, []string{"@alice:localhost"}) {
			t.Fatalf("KeyChanges: wrong user_ids: %v", userIDs)
		}
	})
}

func TestKeyChangesUpperLimit(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db := mustCreateKeyDatabase(ctx, svc, t, testOpts)

		deviceChangeIDA, err := db.StoreKeyChange(ctx, "@alice:localhost")
		MustNotError(t, err)
		deviceChangeIDB, err := db.StoreKeyChange(ctx, "@bob:localhost")
		MustNotError(t, err)
		_, err = db.StoreKeyChange(ctx, "@charlie:localhost")
		MustNotError(t, err)
		userIDs, latest, err := db.KeyChanges(ctx, deviceChangeIDA, deviceChangeIDB)
		if err != nil {
			t.Fatalf("Failed to KeyChanges: %s", err)
		}
		if latest != deviceChangeIDB {
			t.Fatalf("KeyChanges: got latest=%d want %d", latest, deviceChangeIDB)
		}
		if !reflect.DeepEqual(userIDs, []string{"@bob:localhost"}) {
			t.Fatalf("KeyChanges: wrong user_ids: %v", userIDs)
		}
	})
}

var dbLock sync.Mutex
var deviceArray = []string{"AAA", "another_device"}

// The purpose of this test is to make sure that the storage layer is generating sequential stream IDs per user,
// and that they are returned correctly when querying for device keys.
func TestDeviceKeysStreamIDGeneration(t *testing.T) {
	var err error
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db := mustCreateKeyDatabase(ctx, svc, t, testOpts)

		alice := "@alice:TestDeviceKeysStreamIDGeneration"
		bob := "@bob:TestDeviceKeysStreamIDGeneration"
		msgs := []api.DeviceMessage{
			{
				Type: api.TypeDeviceKeyUpdate,
				DeviceKeys: &api.DeviceKeys{
					DeviceID: "AAA",
					UserID:   alice,
					KeyJSON:  []byte(`{"key":"v1"}`),
				},
				// StreamID: 1
			},
			{
				Type: api.TypeDeviceKeyUpdate,
				DeviceKeys: &api.DeviceKeys{
					DeviceID: "AAA",
					UserID:   bob,
					KeyJSON:  []byte(`{"key":"v1"}`),
				},
				// StreamID: 1 as this is a different user
			},
			{
				Type: api.TypeDeviceKeyUpdate,
				DeviceKeys: &api.DeviceKeys{
					DeviceID: "another_device",
					UserID:   alice,
					KeyJSON:  []byte(`{"key":"v1"}`),
				},
				// StreamID: 2 as this is a 2nd device key
			},
		}
		MustNotError(t, db.StoreLocalDeviceKeys(ctx, msgs))
		if msgs[0].StreamID != 1 {
			t.Fatalf("Expected StoreLocalDeviceKeys to set StreamID=1 but got %d", msgs[0].StreamID)
		}
		if msgs[1].StreamID != 1 {
			t.Fatalf("Expected StoreLocalDeviceKeys to set StreamID=1 (different user) but got %d", msgs[1].StreamID)
		}
		if msgs[2].StreamID != 2 {
			t.Fatalf("Expected StoreLocalDeviceKeys to set StreamID=2 (another device) but got %d", msgs[2].StreamID)
		}

		// updating a device sets the next stream ID for that user
		msgs = []api.DeviceMessage{
			{
				Type: api.TypeDeviceKeyUpdate,
				DeviceKeys: &api.DeviceKeys{
					DeviceID: "AAA",
					UserID:   alice,
					KeyJSON:  []byte(`{"key":"v2"}`),
				},
				// StreamID: 3
			},
		}
		MustNotError(t, db.StoreLocalDeviceKeys(ctx, msgs))
		if msgs[0].StreamID != 3 {
			t.Fatalf("Expected StoreLocalDeviceKeys to set StreamID=3 (new key same device) but got %d", msgs[0].StreamID)
		}

		dbLock.Lock()
		defer dbLock.Unlock()
		// Querying for device keys returns the latest stream IDs
		msgs, err = db.DeviceKeysForUser(ctx, alice, deviceArray, false)

		if err != nil {
			t.Fatalf("DeviceKeysForUser returned error: %s", err)
		}
		wantStreamIDs := map[string]int64{
			"AAA":            3,
			"another_device": 2,
		}
		if len(msgs) != len(wantStreamIDs) {
			t.Fatalf("DeviceKeysForUser: wrong number of devices, got %d want %d", len(msgs), len(wantStreamIDs))
		}
		for _, m := range msgs {
			if m.StreamID != wantStreamIDs[m.DeviceID] {
				t.Errorf("DeviceKeysForUser: wrong returned stream ID for key, got %d want %d", m.StreamID, wantStreamIDs[m.DeviceID])
			}
		}
	})
}

func TestOneTimeKeys(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db := mustCreateKeyDatabase(ctx, svc, t, testOpts)

		userID := "@alice:localhost"
		deviceID := "alice_device"
		otk := api.OneTimeKeys{
			UserID:   userID,
			DeviceID: deviceID,
			KeyJSON:  map[string]json.RawMessage{"curve25519:KEY1": []byte(`{"key":"v1"}`)},
		}

		// Add a one time key to the Cm
		_, err := db.StoreOneTimeKeys(ctx, otk)
		MustNotError(t, err)

		// Check the count of one time keys is correct
		count, err := db.OneTimeKeysCount(ctx, userID, deviceID)
		MustNotError(t, err)
		if count.KeyCount["curve25519"] != 1 {
			t.Fatalf("Expected 1 key, got %d", count.KeyCount["curve25519"])
		}

		// Check the actual key contents are correct
		keysJSON, err := db.ExistingOneTimeKeys(ctx, userID, deviceID, []string{"curve25519:KEY1"})
		MustNotError(t, err)
		keyJSON, err := keysJSON["curve25519:KEY1"].MarshalJSON()
		MustNotError(t, err)
		if !bytes.Equal(keyJSON, []byte(`{"key":"v1"}`)) {
			t.Fatalf("Existing keys do not match expected. Got %v", keysJSON["curve25519:KEY1"])
		}

		// Claim a one time key from the database. This should remove it from the database.
		claimedKeys, err := db.ClaimKeys(ctx, map[string]map[string]string{userID: {deviceID: "curve25519"}})
		MustNotError(t, err)

		// Check the claimed key contents are correct
		if !reflect.DeepEqual(claimedKeys[0], otk) {
			t.Fatalf("Expected to claim stored key %v. Got %v", otk, claimedKeys[0])
		}

		// Check the count of one time keys is now zero
		count, err = db.OneTimeKeysCount(ctx, userID, deviceID)
		MustNotError(t, err)
		if count.KeyCount["curve25519"] != 0 {
			t.Fatalf("Expected 0 keys, got %d", count.KeyCount["curve25519"])
		}
	})
}
