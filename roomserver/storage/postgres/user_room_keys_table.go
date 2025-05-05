// Copyright 2023 The Global.org Foundation C.I.C.
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
	"database/sql"
	"errors"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/lib/pq"
)

const userRoomKeysSchema = `
CREATE TABLE IF NOT EXISTS roomserver_user_room_keys (     
    user_nid    INTEGER NOT NULL,
    room_nid    INTEGER NOT NULL,
    pseudo_id_key BYTEA NULL, -- may be null for users not local to the server
    pseudo_id_pub_key BYTEA NOT NULL,
    CONSTRAINT roomserver_user_room_keys_pk PRIMARY KEY (user_nid, room_nid)
);
`

const userRoomKeysSchemaRevert = `DROP TABLE IF EXISTS roomserver_user_room_keys;`

const insertUserRoomPrivateKeySQL = `
	INSERT INTO roomserver_user_room_keys (user_nid, room_nid, pseudo_id_key, pseudo_id_pub_key) VALUES ($1, $2, $3, $4)
	ON CONFLICT ON CONSTRAINT roomserver_user_room_keys_pk DO UPDATE SET pseudo_id_key = roomserver_user_room_keys.pseudo_id_key
	RETURNING (pseudo_id_key)
`

const insertUserRoomPublicKeySQL = `
	INSERT INTO roomserver_user_room_keys (user_nid, room_nid, pseudo_id_pub_key) VALUES ($1, $2, $3)
	ON CONFLICT ON CONSTRAINT roomserver_user_room_keys_pk DO UPDATE SET pseudo_id_pub_key = $3
	RETURNING (pseudo_id_pub_key)
`

const selectUserRoomKeySQL = `SELECT pseudo_id_key FROM roomserver_user_room_keys WHERE user_nid = $1 AND room_nid = $2`

const selectUserRoomPublicKeySQL = `SELECT pseudo_id_pub_key FROM roomserver_user_room_keys WHERE user_nid = $1 AND room_nid = $2`

const selectUserNIDsSQL = `SELECT user_nid, room_nid, pseudo_id_pub_key FROM roomserver_user_room_keys WHERE room_nid = ANY($1) AND pseudo_id_pub_key = ANY($2)`

const selectAllUserRoomPublicKeyForUserSQL = `SELECT room_nid, pseudo_id_pub_key FROM roomserver_user_room_keys WHERE user_nid = $1`

// Refactored table struct for GORM
// All SQL strings are struct fields, set at initialization

type userRoomKeysTable struct {
	cm *sqlutil.Connections

	insertUserRoomPrivateKeySQL          string
	insertUserRoomPublicKeySQL           string
	selectUserRoomKeySQL                 string
	selectUserRoomPublicKeySQL           string
	selectUserNIDsSQL                    string
	selectAllUserRoomPublicKeyForUserSQL string
}

func NewPostgresUserRoomKeysTable(cm *sqlutil.Connections) tables.UserRoomKeys {
	return &userRoomKeysTable{
		cm:                                   cm,
		insertUserRoomPrivateKeySQL:          insertUserRoomPrivateKeySQL,
		insertUserRoomPublicKeySQL:           insertUserRoomPublicKeySQL,
		selectUserRoomKeySQL:                 selectUserRoomKeySQL,
		selectUserRoomPublicKeySQL:           selectUserRoomPublicKeySQL,
		selectUserNIDsSQL:                    selectUserNIDsSQL,
		selectAllUserRoomPublicKeyForUserSQL: selectAllUserRoomPublicKeyForUserSQL,
	}
}

func (t *userRoomKeysTable) InsertUserRoomPrivatePublicKey(ctx context.Context, userNID types.EventStateKeyNID, roomNID types.RoomNID, key ed25519.PrivateKey) (result ed25519.PrivateKey, err error) {
	db := t.cm.Connection(ctx, false)
	var retKey []byte
	err = db.Raw(t.insertUserRoomPrivateKeySQL, userNID, roomNID, key, key.Public()).Scan(&retKey).Error
	if err != nil {
		return nil, err
	}
	return ed25519.PrivateKey(retKey), nil
}

func (t *userRoomKeysTable) InsertUserRoomPublicKey(ctx context.Context, userNID types.EventStateKeyNID, roomNID types.RoomNID, key ed25519.PublicKey) (result ed25519.PublicKey, err error) {
	db := t.cm.Connection(ctx, false)
	var retKey []byte
	err = db.Raw(t.insertUserRoomPublicKeySQL, userNID, roomNID, key).Scan(&retKey).Error
	if err != nil {
		return nil, err
	}
	return ed25519.PublicKey(retKey), nil
}

func (t *userRoomKeysTable) SelectUserRoomPrivateKey(ctx context.Context, userNID types.EventStateKeyNID, roomNID types.RoomNID) (ed25519.PrivateKey, error) {
	db := t.cm.Connection(ctx, true)
	var result []byte
	err := db.Raw(t.selectUserRoomKeySQL, userNID, roomNID).Scan(&result).Error
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return ed25519.PrivateKey(result), nil
}

func (t *userRoomKeysTable) SelectUserRoomPublicKey(ctx context.Context, userNID types.EventStateKeyNID, roomNID types.RoomNID) (ed25519.PublicKey, error) {
	db := t.cm.Connection(ctx, true)
	var result []byte
	err := db.Raw(t.selectUserRoomPublicKeySQL, userNID, roomNID).Scan(&result).Error
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return ed25519.PublicKey(result), nil
}

func (t *userRoomKeysTable) BulkSelectUserNIDs(ctx context.Context, senderKeys map[types.RoomNID][]ed25519.PublicKey) (map[string]types.UserRoomKeyPair, error) {
	db := t.cm.Connection(ctx, true)
	roomNIDs := make([]types.RoomNID, 0, len(senderKeys))
	var senders [][]byte
	for roomNID := range senderKeys {
		roomNIDs = append(roomNIDs, roomNID)
		for _, key := range senderKeys[roomNID] {
			senders = append(senders, key)
		}
	}
	rows, err := db.Raw(t.selectUserNIDsSQL, pq.Array(roomNIDs), pq.Array(senders)).Rows()
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

func (t *userRoomKeysTable) SelectAllPublicKeysForUser(ctx context.Context, userNID types.EventStateKeyNID) (map[types.RoomNID]ed25519.PublicKey, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectAllUserRoomPublicKeyForUserSQL, userNID).Rows()
	if errors.Is(err, sql.ErrNoRows) {
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
