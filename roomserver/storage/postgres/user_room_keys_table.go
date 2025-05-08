// Copyright 2023 The Matrix.org Foundation C.I.C.
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

package postgres

import (
	"context"
	"crypto/ed25519"
	"errors"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/lib/pq"
	"github.com/pitabwire/frame"
	"gorm.io/gorm"
)

// SQL queries for user room keys table
const (
	userRoomKeysSchema = `
CREATE TABLE IF NOT EXISTS roomserver_user_room_keys (     
    user_nid    INTEGER NOT NULL,
    room_nid    INTEGER NOT NULL,
    pseudo_id_key BYTEA NULL, -- may be null for users not local to the server
    pseudo_id_pub_key BYTEA NOT NULL,
    CONSTRAINT roomserver_user_room_keys_pk PRIMARY KEY (user_nid, room_nid)
);
`

	userRoomKeysSchemaRevert = "DROP TABLE IF EXISTS roomserver_user_room_keys;"

	insertUserRoomPrivateKeySQL = `
	INSERT INTO roomserver_user_room_keys (user_nid, room_nid, pseudo_id_key, pseudo_id_pub_key) VALUES ($1, $2, $3, $4)
	ON CONFLICT ON CONSTRAINT roomserver_user_room_keys_pk DO UPDATE SET pseudo_id_key = roomserver_user_room_keys.pseudo_id_key
	RETURNING (pseudo_id_key)
`

	insertUserRoomPublicKeySQL = `
	INSERT INTO roomserver_user_room_keys (user_nid, room_nid, pseudo_id_pub_key) VALUES ($1, $2, $3)
	ON CONFLICT ON CONSTRAINT roomserver_user_room_keys_pk DO UPDATE SET pseudo_id_pub_key = $3
	RETURNING (pseudo_id_pub_key)
`

	selectUserRoomKeySQL = `SELECT pseudo_id_key FROM roomserver_user_room_keys WHERE user_nid = $1 AND room_nid = $2`

	selectUserRoomPublicKeySQL = `SELECT pseudo_id_pub_key FROM roomserver_user_room_keys WHERE user_nid = $1 AND room_nid = $2`

	selectUserNIDsSQL = `SELECT user_nid, room_nid, pseudo_id_pub_key FROM roomserver_user_room_keys WHERE room_nid = ANY($1) AND pseudo_id_pub_key = ANY($2)`

	selectAllUserRoomPublicKeyForUserSQL = `SELECT room_nid, pseudo_id_pub_key FROM roomserver_user_room_keys WHERE user_nid = $1`
)

// userRoomKeysStatements holds the SQL queries for user room keys
type userRoomKeysStatements struct {
	cm *sqlutil.Connections

	// SQL query string fields
	insertUserRoomPrivateKeySQL          string
	insertUserRoomPublicKeySQL           string
	selectUserRoomKeySQL                 string
	selectUserRoomPublicKeySQL           string
	selectUserNIDsSQL                    string
	selectAllUserRoomPublicKeyForUserSQL string
}

// NewPostgresUserRoomKeysTable creates a new instance of the user room keys table.
// It creates the table if it doesn't exist and applies any necessary migrations.
func NewPostgresUserRoomKeysTable(ctx context.Context, cm *sqlutil.Connections) (tables.UserRoomKeys, error) {
	s := &userRoomKeysStatements{
		cm: cm,

		insertUserRoomPrivateKeySQL:          insertUserRoomPrivateKeySQL,
		insertUserRoomPublicKeySQL:           insertUserRoomPublicKeySQL,
		selectUserRoomKeySQL:                 selectUserRoomKeySQL,
		selectUserRoomPublicKeySQL:           selectUserRoomPublicKeySQL,
		selectUserNIDsSQL:                    selectUserNIDsSQL,
		selectAllUserRoomPublicKeyForUserSQL: selectAllUserRoomPublicKeyForUserSQL,
	}

	// Create the table if it doesn't exist using migration
	err := cm.MigrateStrings(ctx, frame.MigrationPatch{
		Name:        "roomserver_user_room_keys_schema_001",
		Patch:       userRoomKeysSchema,
		RevertPatch: userRoomKeysSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return s, nil
}

// InsertUserRoomPrivatePublicKey inserts a user's private and public key for a room.
func (s *userRoomKeysStatements) InsertUserRoomPrivatePublicKey(
	ctx context.Context,
	userNID types.EventStateKeyNID,
	roomNID types.RoomNID,
	key ed25519.PrivateKey,
) (result ed25519.PrivateKey, err error) {
	db := s.cm.Connection(ctx, false)

	row := db.Raw(s.insertUserRoomPrivateKeySQL, userNID, roomNID, key, key.Public()).Row()
	err = row.Scan(&result)
	return result, err
}

// InsertUserRoomPublicKey inserts a user's public key for a room.
func (s *userRoomKeysStatements) InsertUserRoomPublicKey(
	ctx context.Context,
	userNID types.EventStateKeyNID,
	roomNID types.RoomNID,
	key ed25519.PublicKey,
) (result ed25519.PublicKey, err error) {
	db := s.cm.Connection(ctx, false)

	row := db.Raw(s.insertUserRoomPublicKeySQL, userNID, roomNID, key).Row()
	err = row.Scan(&result)
	return result, err
}

// SelectUserRoomPrivateKey retrieves a user's private key for a room.
func (s *userRoomKeysStatements) SelectUserRoomPrivateKey(
	ctx context.Context,
	userNID types.EventStateKeyNID,
	roomNID types.RoomNID,
) (ed25519.PrivateKey, error) {
	db := s.cm.Connection(ctx, true)

	var result ed25519.PrivateKey
	row := db.Raw(s.selectUserRoomKeySQL, userNID, roomNID).Row()
	err := row.Scan(&result)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return result, err
}

// SelectUserRoomPublicKey retrieves a user's public key for a room.
func (s *userRoomKeysStatements) SelectUserRoomPublicKey(
	ctx context.Context,
	userNID types.EventStateKeyNID,
	roomNID types.RoomNID,
) (ed25519.PublicKey, error) {
	db := s.cm.Connection(ctx, true)

	var result ed25519.PublicKey
	row := db.Raw(s.selectUserRoomPublicKeySQL, userNID, roomNID).Row()
	err := row.Scan(&result)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return result, err
}

// BulkSelectUserNIDs retrieves user NIDs for given room NIDs and sender keys.
func (s *userRoomKeysStatements) BulkSelectUserNIDs(
	ctx context.Context,
	senderKeys map[types.RoomNID][]ed25519.PublicKey,
) (map[string]types.UserRoomKeyPair, error) {
	db := s.cm.Connection(ctx, true)

	roomNIDs := make([]types.RoomNID, 0, len(senderKeys))
	var senders [][]byte
	for roomNID := range senderKeys {
		roomNIDs = append(roomNIDs, roomNID)
		for _, key := range senderKeys[roomNID] {
			senders = append(senders, key)
		}
	}

	rows, err := db.Raw(s.selectUserNIDsSQL, pq.Array(roomNIDs), pq.Array(senders)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "failed to close rows")

	result := make(map[string]types.UserRoomKeyPair, len(senders)+len(roomNIDs))
	var publicKey []byte
	userRoomKeyPair := types.UserRoomKeyPair{}
	for rows.Next() {
		if err = rows.Scan(&userRoomKeyPair.EventStateKeyNID, &userRoomKeyPair.RoomNID, &publicKey); err != nil {
			return nil, err
		}
		result[spec.Base64Bytes(publicKey).Encode()] = userRoomKeyPair
	}
	return result, rows.Err()
}

// SelectAllPublicKeysForUser retrieves all public keys for a user.
func (s *userRoomKeysStatements) SelectAllPublicKeysForUser(
	ctx context.Context,
	userNID types.EventStateKeyNID,
) (map[types.RoomNID]ed25519.PublicKey, error) {
	db := s.cm.Connection(ctx, true)

	rows, err := db.Raw(s.selectAllUserRoomPublicKeyForUserSQL, userNID).Rows()
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectAllPublicKeysForUser: failed to close rows")

	resultMap := make(map[types.RoomNID]ed25519.PublicKey)

	var roomNID types.RoomNID
	var pubkey ed25519.PublicKey
	for rows.Next() {
		if err = rows.Scan(&roomNID, &pubkey); err != nil {
			return nil, err
		}
		resultMap[roomNID] = pubkey
	}
	return resultMap, rows.Err()
}
