// Copyright 2017 Vector Creations Ltd
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

package shared

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	clientapi "github.com/antinvestor/matrix/clientapi/api"
	"github.com/antinvestor/matrix/clientapi/auth/authtypes"
	"github.com/antinvestor/matrix/internal/pushrules"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"github.com/antinvestor/matrix/userapi/types"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/oauth2"
)

// Database represents an account database
type Database struct {
	Cm                    sqlutil.ConnectionManager
	RegistrationTokens    tables.RegistrationTokensTable
	Accounts              tables.AccountsTable
	Profiles              tables.ProfileTable
	AccountDatas          tables.AccountDataTable
	ThreePIDs             tables.ThreePIDTable
	OpenIDTokens          tables.OpenIDTable
	KeyBackups            tables.KeyBackupTable
	KeyBackupVersions     tables.KeyBackupVersionTable
	Devices               tables.DevicesTable
	LoginTokens           tables.LoginTokenTable
	Notifications         tables.NotificationTable
	Pushers               tables.PusherTable
	Stats                 tables.StatsTable
	LoginTokenLifetime    time.Duration
	ServerName            spec.ServerName
	BcryptCost            int
	OpenIDTokenLifetimeMS int64
}

type KeyDatabase struct {
	OneTimeKeysTable      tables.OneTimeKeys
	DeviceKeysTable       tables.DeviceKeys
	KeyChangesTable       tables.KeyChanges
	StaleDeviceListsTable tables.StaleDeviceLists
	CrossSigningKeysTable tables.CrossSigningKeys
	CrossSigningSigsTable tables.CrossSigningSigs
	Cm                    sqlutil.ConnectionManager
}

const (
	// The length of generated device IDs
	deviceIDByteLength   = 6
	loginTokenByteLength = 32
)

func (d *Database) RegistrationTokenExists(ctx context.Context, token string) (bool, error) {
	return d.RegistrationTokens.RegistrationTokenExists(ctx, token)
}

func (d *Database) InsertRegistrationToken(ctx context.Context, registrationToken *clientapi.RegistrationToken) (created bool, err error) {
	err = d.Cm.Do(ctx, func(ctx context.Context) error {
		created, err = d.RegistrationTokens.InsertRegistrationToken(ctx, registrationToken)
		return err
	})
	return
}

func (d *Database) ListRegistrationTokens(ctx context.Context, returnAll bool, valid bool) ([]clientapi.RegistrationToken, error) {
	return d.RegistrationTokens.ListRegistrationTokens(ctx, returnAll, valid)
}

func (d *Database) GetRegistrationToken(ctx context.Context, tokenString string) (*clientapi.RegistrationToken, error) {
	return d.RegistrationTokens.GetRegistrationToken(ctx, tokenString)
}

func (d *Database) DeleteRegistrationToken(ctx context.Context, tokenString string) (err error) {
	err = d.Cm.Do(ctx, func(ctx context.Context) error {
		err = d.RegistrationTokens.DeleteRegistrationToken(ctx, tokenString)
		return err
	})
	return
}

func (d *Database) UpdateRegistrationToken(ctx context.Context, tokenString string, newAttributes map[string]interface{}) (updatedToken *clientapi.RegistrationToken, err error) {
	err = d.Cm.Do(ctx, func(ctx context.Context) error {
		updatedToken, err = d.RegistrationTokens.UpdateRegistrationToken(ctx, tokenString, newAttributes)
		return err
	})
	return
}

// GetAccountByPassword returns the account associated with the given localpart and password.
// Returns sql.ErrNoRows if no account exists which matches the given localpart.
func (d *Database) GetAccountByPassword(
	ctx context.Context, localpart string, serverName spec.ServerName,
	plaintextPassword string,
) (*api.Account, error) {
	hash, err := d.Accounts.SelectPasswordHash(ctx, localpart, serverName)
	if err != nil {
		return nil, err
	}
	if len(hash) == 0 && len(plaintextPassword) > 0 {
		return nil, bcrypt.ErrHashTooShort
	}
	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(plaintextPassword)); err != nil {
		return nil, err
	}
	return d.Accounts.SelectAccountByLocalpart(ctx, localpart, serverName)
}

// GetProfileByLocalpart returns the profile associated with the given localpart.
// Returns sql.ErrNoRows if no profile exists which matches the given localpart.
func (d *Database) GetProfileByLocalpart(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
) (*authtypes.Profile, error) {
	return d.Profiles.SelectProfileByLocalpart(ctx, localpart, serverName)
}

// SetAvatarURL updates the avatar URL of the profile associated with the given
// localpart. Returns an error if something went wrong with the SQL query
func (d *Database) SetAvatarURL(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	avatarURL string,
) (profile *authtypes.Profile, changed bool, err error) {
	err = d.Cm.Do(ctx, func(ctx context.Context) error {
		profile, changed, err = d.Profiles.SetAvatarURL(ctx, localpart, serverName, avatarURL)
		return err
	})
	return
}

// SetDisplayName updates the display name of the profile associated with the given
// localpart. Returns an error if something went wrong with the SQL query
func (d *Database) SetDisplayName(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	displayName string,
) (profile *authtypes.Profile, changed bool, err error) {
	err = d.Cm.Do(ctx, func(ctx context.Context) error {
		profile, changed, err = d.Profiles.SetDisplayName(ctx, localpart, serverName, displayName)
		return err
	})
	return
}

// SetPassword sets the account password to the given hash.
func (d *Database) SetPassword(
	ctx context.Context, localpart string, serverName spec.ServerName,
	plaintextPassword string,
) error {
	hash, err := d.hashPassword(plaintextPassword)
	if err != nil {
		return err
	}
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.Accounts.UpdatePassword(ctx, localpart, serverName, hash)
	})
}

// CreateAccount makes a new account with the given login name and password, and creates an empty profile
// for this account. If no password is supplied, the account will be a passwordless account. If the
// account already exists, it will return nil, ErrUserExists.
func (d *Database) CreateAccount(
	ctx context.Context, localpart string, serverName spec.ServerName,
	plaintextPassword, appserviceID string, accountType api.AccountType,
) (acc *api.Account, err error) {
	err = d.Cm.Do(ctx, func(ctx context.Context) error {
		// For guest accounts, we create a new numeric local part
		if accountType == api.AccountTypeGuest {
			var numLocalpart int64
			numLocalpart, err = d.Accounts.SelectNewNumericLocalpart(ctx, serverName)
			if err != nil {
				return fmt.Errorf("d.Accounts.SelectNewNumericLocalpart: %w", err)
			}
			localpart = strconv.FormatInt(numLocalpart, 10)
			plaintextPassword = ""
			appserviceID = ""
		}
		acc, err = d.createAccount(ctx, localpart, serverName, plaintextPassword, appserviceID, accountType)
		return err
	})
	return
}

// WARNING! This function assumes that the relevant mutexes have already
// been taken out by the caller (e.g. CreateAccount or CreateGuestAccount).
func (d *Database) createAccount(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	plaintextPassword, appserviceID string, accountType api.AccountType,
) (*api.Account, error) {
	var err error
	var account *api.Account
	// Generate a password hash if this is not a password-less user
	hash := ""
	if plaintextPassword != "" {
		hash, err = d.hashPassword(plaintextPassword)
		if err != nil {
			return nil, err
		}
	}
	if account, err = d.Accounts.InsertAccount(ctx, localpart, serverName, hash, appserviceID, accountType); err != nil {
		return nil, sqlutil.ErrUserExists
	}
	if err = d.Profiles.InsertProfile(ctx, localpart, serverName); err != nil {
		return nil, fmt.Errorf("d.Profiles.InsertProfile: %w", err)
	}
	pushRuleSets := pushrules.DefaultAccountRuleSets(localpart, serverName)
	prbs, err := json.Marshal(pushRuleSets)
	if err != nil {
		return nil, fmt.Errorf("json.Marshal: %w", err)
	}
	if err = d.AccountDatas.InsertAccountData(ctx, localpart, serverName, "", "m.push_rules", prbs); err != nil {
		return nil, fmt.Errorf("d.AccountDatas.InsertAccountData: %w", err)
	}
	return account, nil
}

func (d *Database) QueryPushRules(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
) (*pushrules.AccountRuleSets, error) {
	data, err := d.AccountDatas.SelectAccountDataByType(ctx, localpart, serverName, "", "m.push_rules")
	if err != nil {
		return nil, err
	}

	// If we didn't find any default push rules then we should just generate some
	// fresh ones.
	if len(data) == 0 {
		pushRuleSets := pushrules.DefaultAccountRuleSets(localpart, serverName)
		prbs, err := json.Marshal(pushRuleSets)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal default push rules: %w", err)
		}
		err = d.Cm.Do(ctx, func(ctx context.Context) error {
			if dbErr := d.AccountDatas.InsertAccountData(ctx, localpart, serverName, "", "m.push_rules", prbs); dbErr != nil {
				return fmt.Errorf("failed to save default push rules: %w", dbErr)
			}
			return nil
		})

		return pushRuleSets, err
	}

	var pushRules pushrules.AccountRuleSets
	if err := json.Unmarshal(data, &pushRules); err != nil {
		return nil, err
	}

	return &pushRules, nil
}

// SaveAccountData saves new account data for a given user and a given room.
// If the account data is not specific to a room, the room ID should be an empty string
// If an account data already exists for a given set (user, room, data type), it will
// update the corresponding row with the new content
// Returns a SQL error if there was an issue with the insertion/update
func (d *Database) SaveAccountData(
	ctx context.Context, localpart string, serverName spec.ServerName,
	roomID, dataType string, content json.RawMessage,
) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.AccountDatas.InsertAccountData(ctx, localpart, serverName, roomID, dataType, content)
	})
}

// GetAccountData returns account data related to a given localpart
// If no account data could be found, returns an empty arrays
// Returns an error if there was an issue with the retrieval
func (d *Database) GetAccountData(ctx context.Context, localpart string, serverName spec.ServerName) (
	global map[string]json.RawMessage,
	rooms map[string]map[string]json.RawMessage,
	err error,
) {
	return d.AccountDatas.SelectAccountData(ctx, localpart, serverName)
}

// GetAccountDataByType returns account data matching a given
// localpart, room ID and type.
// If no account data could be found, returns nil
// Returns an error if there was an issue with the retrieval
func (d *Database) GetAccountDataByType(
	ctx context.Context, localpart string, serverName spec.ServerName,
	roomID, dataType string,
) (data json.RawMessage, err error) {
	return d.AccountDatas.SelectAccountDataByType(
		ctx, localpart, serverName, roomID, dataType,
	)
}

// GetNewNumericLocalpart generates and returns a new unused numeric localpart
func (d *Database) GetNewNumericLocalpart(
	ctx context.Context, serverName spec.ServerName,
) (int64, error) {
	return d.Accounts.SelectNewNumericLocalpart(ctx, serverName)
}

func (d *Database) hashPassword(plaintext string) (hash string, err error) {
	hashBytes, err := bcrypt.GenerateFromPassword([]byte(plaintext), d.BcryptCost)
	return string(hashBytes), err
}

// Err3PIDInUse is the error returned when trying to save an association involving
// a third-party identifier which is already associated to a local user.
var Err3PIDInUse = errors.New("this third-party identifier is already in use")

// SaveThreePIDAssociation saves the association between a third party identifier
// and a local Global user (identified by the user's ID's local part).
// If the third-party identifier is already part of an association, returns Err3PIDInUse.
// Returns an error if there was a problem talking to the database.
func (d *Database) SaveThreePIDAssociation(
	ctx context.Context, threepid string,
	localpart string, serverName spec.ServerName,
	medium string,
) (err error) {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		user, _, err := d.ThreePIDs.SelectLocalpartForThreePID(
			ctx, threepid, medium,
		)
		if err != nil {
			return err
		}

		if len(user) > 0 {
			return Err3PIDInUse
		}

		return d.ThreePIDs.InsertThreePID(ctx, threepid, medium, localpart, serverName)
	})
}

// RemoveThreePIDAssociation removes the association involving a given third-party
// identifier.
// If no association exists involving this third-party identifier, returns nothing.
// If there was a problem talking to the database, returns an error.
func (d *Database) RemoveThreePIDAssociation(
	ctx context.Context, threepid string, medium string,
) (err error) {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.ThreePIDs.DeleteThreePID(ctx, threepid, medium)
	})
}

// GetLocalpartForThreePID looks up the localpart associated with a given third-party
// identifier.
// If no association involves the given third-party idenfitier, returns an empty
// string.
// Returns an error if there was a problem talking to the database.
func (d *Database) GetLocalpartForThreePID(
	ctx context.Context, threepid string, medium string,
) (localpart string, serverName spec.ServerName, err error) {
	return d.ThreePIDs.SelectLocalpartForThreePID(ctx, threepid, medium)
}

// GetThreePIDsForLocalpart looks up the third-party identifiers associated with
// a given local user.
// If no association is known for this user, returns an empty slice.
// Returns an error if there was an issue talking to the database.
func (d *Database) GetThreePIDsForLocalpart(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
) (threepids []authtypes.ThreePID, err error) {
	return d.ThreePIDs.SelectThreePIDsForLocalpart(ctx, localpart, serverName)
}

// CheckAccountAvailability checks if the username/localpart is already present
// in the database.
// If the DB returns sql.ErrNoRows the Localpart isn't taken.
func (d *Database) CheckAccountAvailability(ctx context.Context, localpart string, serverName spec.ServerName) (bool, error) {
	_, err := d.Accounts.SelectAccountByLocalpart(ctx, localpart, serverName)
	if sqlutil.ErrorIsNoRows(err) {
		return true, nil
	}
	return false, err
}

// GetAccountByLocalpart returns the account associated with the given localpart.
// This function assumes the request is authenticated or the account data is used only internally.
// Returns sql.ErrNoRows if no account exists which matches the given localpart.
func (d *Database) GetAccountByLocalpart(ctx context.Context, localpart string, serverName spec.ServerName,
) (*api.Account, error) {
	// try to get the account with lowercase localpart (majority)
	acc, err := d.Accounts.SelectAccountByLocalpart(ctx, strings.ToLower(localpart), serverName)
	if sqlutil.ErrorIsNoRows(err) {
		acc, err = d.Accounts.SelectAccountByLocalpart(ctx, localpart, serverName) // try with localpart as passed by the request
	}
	return acc, err
}

// SearchProfiles returns all profiles where the provided localpart or display name
// match any part of the profiles in the database.
func (d *Database) SearchProfiles(ctx context.Context, localpart string, searchString string, limit int,
) ([]authtypes.Profile, error) {
	return d.Profiles.SelectProfilesBySearch(ctx, localpart, searchString, limit)
}

// DeactivateAccount deactivates the user's account, removing all ability for the user to login again.
func (d *Database) DeactivateAccount(ctx context.Context, localpart string, serverName spec.ServerName) (err error) {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.Accounts.DeactivateAccount(ctx, localpart, serverName)
	})
}

// CreateOpenIDToken persists a new token that was issued for OpenID Connect
func (d *Database) CreateOpenIDToken(
	ctx context.Context,
	token, userID string,
) (int64, error) {
	localpart, domain, err := gomatrixserverlib.SplitID('@', userID)
	if err != nil {
		return 0, nil
	}
	expiresAtMS := time.Now().UnixNano()/int64(time.Millisecond) + d.OpenIDTokenLifetimeMS
	err = d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.OpenIDTokens.InsertOpenIDToken(ctx, token, localpart, domain, expiresAtMS)
	})
	return expiresAtMS, err
}

// GetOpenIDTokenAttributes gets the attributes of issued an OIDC auth token
func (d *Database) GetOpenIDTokenAttributes(
	ctx context.Context,
	token string,
) (*api.OpenIDTokenAttributes, error) {
	return d.OpenIDTokens.SelectOpenIDTokenAtrributes(ctx, token)
}

func (d *Database) CreateKeyBackup(
	ctx context.Context, userID, algorithm string, authData json.RawMessage,
) (version string, err error) {
	err = d.Cm.Do(ctx, func(ctx context.Context) error {
		version, err = d.KeyBackupVersions.InsertKeyBackup(ctx, userID, algorithm, authData, "")
		return err
	})
	return
}

func (d *Database) UpdateKeyBackupAuthData(
	ctx context.Context, userID, version string, authData json.RawMessage,
) (err error) {
	err = d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.KeyBackupVersions.UpdateKeyBackupAuthData(ctx, userID, version, authData)
	})
	return
}

func (d *Database) DeleteKeyBackup(
	ctx context.Context, userID, version string,
) (exists bool, err error) {
	err = d.Cm.Do(ctx, func(ctx context.Context) error {
		exists, err = d.KeyBackupVersions.DeleteKeyBackup(ctx, userID, version)
		return err
	})
	return
}

func (d *Database) GetKeyBackup(
	ctx context.Context, userID, version string,
) (versionResult, algorithm string, authData json.RawMessage, etag string, deleted bool, err error) {
	err = d.Cm.Do(ctx, func(ctx context.Context) error {
		versionResult, algorithm, authData, etag, deleted, err = d.KeyBackupVersions.SelectKeyBackup(ctx, userID, version)
		return err
	})
	return
}

func (d *Database) GetBackupKeys(
	ctx context.Context, version, userID, filterRoomID, filterSessionID string,
) (result map[string]map[string]api.KeyBackupSession, err error) {
	err = d.Cm.Do(ctx, func(ctx context.Context) error {
		if filterSessionID != "" {
			result, err = d.KeyBackups.SelectKeysByRoomIDAndSessionID(ctx, userID, version, filterRoomID, filterSessionID)
			return err
		}
		if filterRoomID != "" {
			result, err = d.KeyBackups.SelectKeysByRoomID(ctx, userID, version, filterRoomID)
			return err
		}
		result, err = d.KeyBackups.SelectKeys(ctx, userID, version)
		return err
	})
	return
}

func (d *Database) CountBackupKeys(
	ctx context.Context, version, userID string,
) (count int64, err error) {
	err = d.Cm.Do(ctx, func(ctx context.Context) error {
		count, err = d.KeyBackups.CountKeys(ctx, userID, version)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

// nolint:nakedret
func (d *Database) UpsertBackupKeys(
	ctx context.Context, version, userID string, uploads []api.InternalKeyBackupSession,
) (count int64, etag string, err error) {
	// wrap the following logic in a txn to ensure we atomically upload keys
	err = d.Cm.Do(ctx, func(ctx context.Context) error {
		_, _, _, oldETag, deleted, err := d.KeyBackupVersions.SelectKeyBackup(ctx, userID, version)
		if err != nil {
			return err
		}
		if deleted {
			return fmt.Errorf("backup was deleted")
		}
		// pull out all keys for this (user_id, version)
		existingKeys, err := d.KeyBackups.SelectKeys(ctx, userID, version)
		if err != nil {
			return err
		}

		changed := false
		// loop over all the new keys (which should be smaller than the set of backed up keys)
		for _, newKey := range uploads {
			// if we have a matching (room_id, session_id), we may need to update the key if it meets some rules, check them.
			existingRoom := existingKeys[newKey.RoomID]
			if existingRoom != nil {
				existingSession, ok := existingRoom[newKey.SessionID]
				if ok {
					if existingSession.ShouldReplaceRoomKey(&newKey.KeyBackupSession) {
						err = d.KeyBackups.UpdateBackupKey(ctx, userID, version, newKey)
						changed = true
						if err != nil {
							return fmt.Errorf("d.KeyBackups.UpdateBackupKey: %w", err)
						}
					}
					// if we shouldn't replace the key we do nothing with it
					continue
				}
			}
			// if we're here, either the room or session are new, either way, we insert
			err = d.KeyBackups.InsertBackupKey(ctx, userID, version, newKey)
			changed = true
			if err != nil {
				return fmt.Errorf("d.KeyBackups.InsertBackupKey: %w", err)
			}
		}

		count, err = d.KeyBackups.CountKeys(ctx, userID, version)
		if err != nil {
			return err
		}
		if changed {
			// update the etag
			var newETag string
			if oldETag == "" {
				newETag = "1"
			} else {
				oldETagInt, err := strconv.ParseInt(oldETag, 10, 64)
				if err != nil {
					return fmt.Errorf("failed to parse old etag: %s", err)
				}
				newETag = strconv.FormatInt(oldETagInt+1, 10)
			}
			etag = newETag
			return d.KeyBackupVersions.UpdateKeyBackupETag(ctx, userID, version, newETag)
		} else {
			etag = oldETag
		}

		return nil
	})
	return
}

// GetDeviceByAccessToken returns the device matching the given access token.
// Returns sql.ErrNoRows if no matching device was found.
func (d *Database) GetDeviceByAccessToken(
	ctx context.Context, token string,
) (context.Context, *api.Device, error) {
	return d.Devices.SelectDeviceByToken(ctx, token)
}

// GetDeviceByID returns the device matching the given ID.
// Returns sql.ErrNoRows if no matching device was found.
func (d *Database) GetDeviceByID(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	deviceID string,
) (*api.Device, error) {
	return d.Devices.SelectDeviceByID(ctx, localpart, serverName, deviceID)
}

// GetDevicesByLocalpart returns the devices matching the given localpart.
func (d *Database) GetDevicesByLocalpart(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
) ([]api.Device, error) {
	return d.Devices.SelectDevicesByLocalpart(ctx, localpart, serverName, "")
}

func (d *Database) GetDevicesByID(ctx context.Context, deviceIDs []string) ([]api.Device, error) {
	return d.Devices.SelectDevicesByID(ctx, deviceIDs)
}

// CreateDevice makes a new device associated with the given user ID localpart.
// If there is already a device with the same device ID for this user, that access token will be revoked
// and replaced with the given accessToken. If the given accessToken is already in use for another device,
// an error will be returned.
// If no device ID is given one is generated.
// Returns the device on success.
func (d *Database) CreateDevice(
	ctx context.Context, localpart string, serverName spec.ServerName,
	deviceID *string, accessToken string, extraData *oauth2.Token, displayName *string, ipAddr, userAgent string,
) (dev *api.Device, returnErr error) {
	if deviceID != nil {

		returnErr = d.Cm.Do(ctx, func(ctx context.Context) error {
			var err error
			// Revoke existing tokens for this device
			if err = d.Devices.DeleteDevice(ctx, *deviceID, localpart, serverName); err != nil {
				return err
			}

			dev, err = d.Devices.InsertDevice(ctx, *deviceID, localpart, serverName, accessToken, extraData, displayName, ipAddr, userAgent)
			return err
		})

	} else {
		// We generate device IDs in a loop in case its already taken.
		// We cap this at going round 5 times to ensure we don't spin forever
		var newDeviceID string
		for i := 1; i <= 5; i++ {
			newDeviceID, returnErr = generateDeviceID()
			if returnErr != nil {
				return nil, returnErr
			}

			returnErr = d.Cm.Do(ctx, func(ctx context.Context) error {
				var err error
				dev, err = d.Devices.InsertDevice(ctx, newDeviceID, localpart, serverName, accessToken, extraData, displayName, ipAddr, userAgent)
				return err
			})
			if returnErr == nil {
				return dev, nil
			}
		}
	}
	return dev, returnErr
}

// generateDeviceID creates a new device id. Returns an error if failed to generate
// random bytes.
func generateDeviceID() (string, error) {
	b := make([]byte, deviceIDByteLength)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	// url-safe no padding
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// UpdateDevice updates the given device with the display name.
// Returns SQL error if there are problems and nil on success.
func (d *Database) UpdateDevice(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	deviceID string, displayName *string,
) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.Devices.UpdateDeviceName(ctx, localpart, serverName, deviceID, displayName)
	})
}

// RemoveDevices revokes one or more devices by deleting the entry in the database
// matching with the given device IDs and user ID localpart.
// If the devices don't exist, it will not return an error
// If something went wrong during the deletion, it will return the SQL error.
func (d *Database) RemoveDevices(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	devices []string,
) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		if err := d.Devices.DeleteDevices(ctx, localpart, serverName, devices); !sqlutil.ErrorIsNoRows(err) {
			return err
		}
		return nil
	})
}

// RemoveAllDevices revokes devices by deleting the entry in the
// database matching the given user ID localpart.
// If something went wrong during the deletion, it will return the SQL error.
func (d *Database) RemoveAllDevices(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	exceptDeviceID string,
) (devices []api.Device, err error) {
	err = d.Cm.Do(ctx, func(ctx context.Context) error {
		devices, err = d.Devices.SelectDevicesByLocalpart(ctx, localpart, serverName, exceptDeviceID)
		if err != nil {
			return err
		}
		err = d.Devices.DeleteDevicesByLocalpart(ctx, localpart, serverName, exceptDeviceID)
		if !sqlutil.ErrorIsNoRows(err) {
			return err
		}
		return nil
	})
	return
}

// UpdateDeviceLastSeen updates a last seen timestamp and the ip address.
func (d *Database) UpdateDeviceLastSeen(ctx context.Context, localpart string, serverName spec.ServerName, deviceID, ipAddr, userAgent string) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.Devices.UpdateDeviceLastSeen(ctx, localpart, serverName, deviceID, ipAddr, userAgent)
	})
}

// CreateLoginToken generates a token, stores and returns it. The lifetime is
// determined by the loginTokenLifetime given to the Database constructor.
func (d *Database) CreateLoginToken(ctx context.Context, data *api.LoginTokenData) (*api.LoginTokenMetadata, error) {
	tok, err := generateLoginToken()
	if err != nil {
		return nil, err
	}
	meta := &api.LoginTokenMetadata{
		Token:      tok,
		Expiration: time.Now().Add(d.LoginTokenLifetime),
	}

	err = d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.LoginTokens.InsertLoginToken(ctx, meta, data)
	})
	if err != nil {
		return nil, err
	}

	return meta, nil
}

func generateLoginToken() (string, error) {
	b := make([]byte, loginTokenByteLength)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// RemoveLoginToken removes the named token (and may clean up other expired tokens).
func (d *Database) RemoveLoginToken(ctx context.Context, token string) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.LoginTokens.DeleteLoginToken(ctx, token)
	})
}

// GetLoginTokenDataByToken returns the data associated with the given token.
// May return sql.ErrNoRows.
func (d *Database) GetLoginTokenDataByToken(ctx context.Context, token string) (*api.LoginTokenData, error) {
	return d.LoginTokens.SelectLoginToken(ctx, token)
}

func (d *Database) InsertNotification(ctx context.Context, localpart string, serverName spec.ServerName, eventID string, pos uint64, tweaks map[string]interface{}, n *api.Notification) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.Notifications.Insert(ctx, localpart, serverName, eventID, pos, pushrules.BoolTweakOr(tweaks, pushrules.HighlightTweak, false), n)
	})
}

func (d *Database) DeleteNotificationsUpTo(ctx context.Context, localpart string, serverName spec.ServerName, roomID string, pos uint64) (affected bool, err error) {
	err = d.Cm.Do(ctx, func(ctx context.Context) error {
		affected, err = d.Notifications.DeleteUpTo(ctx, localpart, serverName, roomID, pos)
		return err
	})
	return
}

func (d *Database) SetNotificationsRead(ctx context.Context, localpart string, serverName spec.ServerName, roomID string, pos uint64, b bool) (affected bool, err error) {
	err = d.Cm.Do(ctx, func(ctx context.Context) error {
		affected, err = d.Notifications.UpdateRead(ctx, localpart, serverName, roomID, pos, b)
		return err
	})
	return
}

func (d *Database) GetNotifications(ctx context.Context, localpart string, serverName spec.ServerName, fromID int64, limit int, filter tables.NotificationFilter) ([]*api.Notification, int64, error) {
	return d.Notifications.Select(ctx, localpart, serverName, fromID, limit, filter)
}

func (d *Database) GetNotificationCount(ctx context.Context, localpart string, serverName spec.ServerName, filter tables.NotificationFilter) (int64, error) {
	return d.Notifications.SelectCount(ctx, localpart, serverName, filter)
}

func (d *Database) GetRoomNotificationCounts(ctx context.Context, localpart string, serverName spec.ServerName, roomID string) (total int64, highlight int64, _ error) {
	return d.Notifications.SelectRoomCounts(ctx, localpart, serverName, roomID)
}

func (d *Database) DeleteOldNotifications(ctx context.Context) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.Notifications.Clean(ctx)
	})
}

func (d *Database) UpsertPusher(
	ctx context.Context, p api.Pusher,
	localpart string, serverName spec.ServerName,
) error {
	data, err := json.Marshal(p.Data)
	if err != nil {
		return err
	}
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.Pushers.InsertPusher(
			ctx,
			p.SessionID,
			p.PushKey,
			p.PushKeyTS,
			p.Kind,
			p.AppID,
			p.AppDisplayName,
			p.DeviceDisplayName,
			p.ProfileTag,
			p.Language,
			string(data),
			localpart,
			serverName)
	})
}

// GetPushers returns the pushers matching the given localpart.
func (d *Database) GetPushers(
	ctx context.Context, localpart string, serverName spec.ServerName,
) ([]api.Pusher, error) {
	return d.Pushers.SelectPushers(ctx, localpart, serverName)
}

// RemovePusher deletes one pusher
// Invoked when `append` is true and `kind` is null in
// https://matrix.org/docs/spec/client_server/r0.6.1#post-matrix-client-r0-pushers-set
func (d *Database) RemovePusher(
	ctx context.Context, appid, pushkey, localpart string, serverName spec.ServerName,
) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		err := d.Pushers.DeletePusher(ctx, appid, pushkey, localpart, serverName)
		if sqlutil.ErrorIsNoRows(err) {
			return nil
		}
		return err
	})
}

// RemovePushers deletes all pushers that match given App Id and Push K pair.
// Invoked when `append` parameter is false in
// https://matrix.org/docs/spec/client_server/r0.6.1#post-matrix-client-r0-pushers-set
func (d *Database) RemovePushers(
	ctx context.Context, appid, pushkey string,
) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.Pushers.DeletePushers(ctx, appid, pushkey)
	})
}

// UserStatistics populates types.UserStatistics, used in reports.
func (d *Database) UserStatistics(ctx context.Context) (*types.UserStatistics, *types.DatabaseEngine, error) {
	return d.Stats.UserStatistics(ctx)
}

func (d *Database) UpsertDailyRoomsMessages(ctx context.Context, serverName spec.ServerName, stats types.MessageStats, activeRooms, activeE2EERooms int64) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.Stats.UpsertDailyStats(ctx, serverName, stats, activeRooms, activeE2EERooms)
	})
}

func (d *Database) DailyRoomsMessages(
	ctx context.Context, serverName spec.ServerName,
) (stats types.MessageStats, activeRooms, activeE2EERooms int64, err error) {
	return d.Stats.DailyRoomsMessages(ctx, serverName)
}

//

func (d *KeyDatabase) ExistingOneTimeKeys(ctx context.Context, userID, deviceID string, keyIDsWithAlgorithms []string) (map[string]json.RawMessage, error) {
	return d.OneTimeKeysTable.SelectOneTimeKeys(ctx, userID, deviceID, keyIDsWithAlgorithms)
}

func (d *KeyDatabase) StoreOneTimeKeys(ctx context.Context, keys api.OneTimeKeys) (counts *api.OneTimeKeysCount, err error) {
	_ = d.Cm.Do(ctx, func(ctx context.Context) error {
		counts, err = d.OneTimeKeysTable.InsertOneTimeKeys(ctx, keys)
		return err
	})
	return
}

func (d *KeyDatabase) OneTimeKeysCount(ctx context.Context, userID, deviceID string) (*api.OneTimeKeysCount, error) {
	return d.OneTimeKeysTable.CountOneTimeKeys(ctx, userID, deviceID)
}

func (d *KeyDatabase) DeviceKeysJSON(ctx context.Context, keys []api.DeviceMessage) error {
	return d.DeviceKeysTable.SelectDeviceKeysJSON(ctx, keys)
}

func (d *KeyDatabase) PrevIDsExists(ctx context.Context, userID string, prevIDs []int64) (bool, error) {
	count, err := d.DeviceKeysTable.CountStreamIDsForUser(ctx, userID, prevIDs)
	if err != nil {
		return false, err
	}
	return count == len(prevIDs), nil
}

func (d *KeyDatabase) StoreRemoteDeviceKeys(ctx context.Context, keys []api.DeviceMessage, clearUserIDs []string) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		for _, userID := range clearUserIDs {
			err := d.DeviceKeysTable.DeleteAllDeviceKeys(ctx, userID)
			if err != nil {
				return err
			}
		}
		return d.DeviceKeysTable.InsertDeviceKeys(ctx, keys)
	})
}

func (d *KeyDatabase) StoreLocalDeviceKeys(ctx context.Context, keys []api.DeviceMessage) error {
	// work out the latest stream IDs for each user
	userIDToStreamID := make(map[string]int64)
	for _, k := range keys {
		userIDToStreamID[k.UserID] = 0
	}
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		for userID := range userIDToStreamID {
			streamID, err := d.DeviceKeysTable.SelectMaxStreamIDForUser(ctx, userID)
			if err != nil {
				return err
			}
			userIDToStreamID[userID] = streamID
		}
		// set the stream IDs for each key
		for i := range keys {
			k := keys[i]
			userIDToStreamID[k.UserID]++ // start stream from 1
			k.StreamID = userIDToStreamID[k.UserID]
			keys[i] = k
		}
		return d.DeviceKeysTable.InsertDeviceKeys(ctx, keys)
	})
}

func (d *KeyDatabase) DeviceKeysForUser(ctx context.Context, userID string, deviceIDs []string, includeEmpty bool) ([]api.DeviceMessage, error) {
	return d.DeviceKeysTable.SelectBatchDeviceKeys(ctx, userID, deviceIDs, includeEmpty)
}

func (d *KeyDatabase) ClaimKeys(ctx context.Context, userToDeviceToAlgorithm map[string]map[string]string) ([]api.OneTimeKeys, error) {
	var result []api.OneTimeKeys
	err := d.Cm.Do(ctx, func(ctx context.Context) error {
		for userID, deviceToAlgo := range userToDeviceToAlgorithm {
			for deviceID, algo := range deviceToAlgo {
				keyJSON, err := d.OneTimeKeysTable.SelectAndDeleteOneTimeKey(ctx, userID, deviceID, algo)
				if err != nil {
					return err
				}
				if keyJSON != nil {
					result = append(result, api.OneTimeKeys{
						UserID:   userID,
						DeviceID: deviceID,
						KeyJSON:  keyJSON,
					})
				}
			}
		}
		return nil
	})
	return result, err
}

func (d *KeyDatabase) StoreKeyChange(ctx context.Context, userID string) (id int64, err error) {
	err = d.Cm.Do(ctx, func(ctx context.Context) error {
		id, err = d.KeyChangesTable.InsertKeyChange(ctx, userID)
		return err
	})
	return
}

func (d *KeyDatabase) KeyChanges(ctx context.Context, fromOffset, toOffset int64) (userIDs []string, latestOffset int64, err error) {
	return d.KeyChangesTable.SelectKeyChanges(ctx, fromOffset, toOffset)
}

// StaleDeviceLists returns a list of user IDs ending with the domains provided who have stale device lists.
// If no domains are given, all user IDs with stale device lists are returned.
func (d *KeyDatabase) StaleDeviceLists(ctx context.Context, domains []spec.ServerName) ([]string, error) {
	return d.StaleDeviceListsTable.SelectUserIDsWithStaleDeviceLists(ctx, domains)
}

// MarkDeviceListStale sets the stale bit for this user to isStale.
func (d *KeyDatabase) MarkDeviceListStale(ctx context.Context, userID string, isStale bool) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.StaleDeviceListsTable.InsertStaleDeviceList(ctx, userID, isStale)
	})
}

// DeleteDeviceKeys removes the device keys for a given user/device, and any accompanying
// cross-signing signatures relating to that device.
func (d *KeyDatabase) DeleteDeviceKeys(ctx context.Context, userID string, deviceIDs []gomatrixserverlib.KeyID) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		for _, deviceID := range deviceIDs {
			if err := d.CrossSigningSigsTable.DeleteCrossSigningSigsForTarget(ctx, userID, deviceID); err != nil && !sqlutil.ErrorIsNoRows(err) {
				return fmt.Errorf("d.CrossSigningSigsTable.DeleteCrossSigningSigsForTarget: %w", err)
			}
			if err := d.DeviceKeysTable.DeleteDeviceKeys(ctx, userID, string(deviceID)); err != nil && !sqlutil.ErrorIsNoRows(err) {
				return fmt.Errorf("d.DeviceKeysTable.DeleteDeviceKeys: %w", err)
			}
			if err := d.OneTimeKeysTable.DeleteOneTimeKeys(ctx, userID, string(deviceID)); err != nil && !sqlutil.ErrorIsNoRows(err) {
				return fmt.Errorf("d.OneTimeKeysTable.DeleteOneTimeKeys: %w", err)
			}
		}
		return nil
	})
}

// CrossSigningKeysForUser returns the latest known cross-signing keys for a user, if any.
func (d *KeyDatabase) CrossSigningKeysForUser(ctx context.Context, userID string) (map[fclient.CrossSigningKeyPurpose]fclient.CrossSigningKey, error) {
	keyMap, err := d.CrossSigningKeysTable.SelectCrossSigningKeysForUser(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("d.CrossSigningKeysTable.SelectCrossSigningKeysForUser: %w", err)
	}
	results := map[fclient.CrossSigningKeyPurpose]fclient.CrossSigningKey{}
	for purpose, key := range keyMap {
		keyID := gomatrixserverlib.KeyID("ed25519:" + key.Encode())
		result := fclient.CrossSigningKey{
			UserID: userID,
			Usage:  []fclient.CrossSigningKeyPurpose{purpose},
			Keys: map[gomatrixserverlib.KeyID]spec.Base64Bytes{
				keyID: key,
			},
		}
		sigMap, err := d.CrossSigningSigsTable.SelectCrossSigningSigsForTarget(ctx, userID, userID, keyID)
		if err != nil {
			continue
		}
		for sigUserID, forSigUserID := range sigMap {
			if userID != sigUserID {
				continue
			}
			if result.Signatures == nil {
				result.Signatures = map[string]map[gomatrixserverlib.KeyID]spec.Base64Bytes{}
			}
			if _, ok := result.Signatures[sigUserID]; !ok {
				result.Signatures[sigUserID] = map[gomatrixserverlib.KeyID]spec.Base64Bytes{}
			}
			for sigKeyID, sigBytes := range forSigUserID {
				result.Signatures[sigUserID][sigKeyID] = sigBytes
			}
		}
		results[purpose] = result
	}
	return results, nil
}

// CrossSigningKeysDataForUser returns the latest known cross-signing keys for a user, if any.
func (d *KeyDatabase) CrossSigningKeysDataForUser(ctx context.Context, userID string) (types.CrossSigningKeyMap, error) {
	return d.CrossSigningKeysTable.SelectCrossSigningKeysForUser(ctx, userID)
}

// CrossSigningSigsForTarget returns the signatures for a given user's key ID, if any.
func (d *KeyDatabase) CrossSigningSigsForTarget(ctx context.Context, originUserID, targetUserID string, targetKeyID gomatrixserverlib.KeyID) (types.CrossSigningSigMap, error) {
	return d.CrossSigningSigsTable.SelectCrossSigningSigsForTarget(ctx, originUserID, targetUserID, targetKeyID)
}

// StoreCrossSigningKeysForUser stores the latest known cross-signing keys for a user.
func (d *KeyDatabase) StoreCrossSigningKeysForUser(ctx context.Context, userID string, keyMap types.CrossSigningKeyMap) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		for keyType, keyData := range keyMap {
			if err := d.CrossSigningKeysTable.UpsertCrossSigningKeysForUser(ctx, userID, keyType, keyData); err != nil {
				return fmt.Errorf("d.CrossSigningKeysTable.InsertCrossSigningKeysForUser: %w", err)
			}
		}
		return nil
	})
}

// StoreCrossSigningSigsForTarget stores a signature for a target user ID and key/dvice.
func (d *KeyDatabase) StoreCrossSigningSigsForTarget(
	ctx context.Context,
	originUserID string, originKeyID gomatrixserverlib.KeyID,
	targetUserID string, targetKeyID gomatrixserverlib.KeyID,
	signature spec.Base64Bytes,
) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		if err := d.CrossSigningSigsTable.UpsertCrossSigningSigsForTarget(ctx, originUserID, originKeyID, targetUserID, targetKeyID, signature); err != nil {
			return fmt.Errorf("d.CrossSigningSigsTable.InsertCrossSigningSigsForTarget: %w", err)
		}
		return nil
	})
}

// DeleteStaleDeviceLists deletes stale device list entries for users we don't share a room with anymore.
func (d *KeyDatabase) DeleteStaleDeviceLists(
	ctx context.Context,
	userIDs []string,
) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.StaleDeviceListsTable.DeleteStaleDeviceLists(ctx, userIDs)
	})
}
