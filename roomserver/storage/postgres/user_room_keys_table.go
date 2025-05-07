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

// SQL query constants for user room keys operations
const (
	// insertUserRoomPrivateKeySQL inserts a user's private key for a room
	insertUserRoomPrivateKeySQL = `
		INSERT INTO roomserver_user_room_keys (user_nid, room_nid, pseudo_id_key, pseudo_id_pub_key) VALUES ($1, $2, $3, $4)
		ON CONFLICT ON CONSTRAINT roomserver_user_room_keys_pk DO UPDATE SET pseudo_id_key = roomserver_user_room_keys.pseudo_id_key
		RETURNING (pseudo_id_key)
	`

	// insertUserRoomPublicKeySQL inserts a user's public key for a room
	insertUserRoomPublicKeySQL = `
		INSERT INTO roomserver_user_room_keys (user_nid, room_nid, pseudo_id_pub_key) VALUES ($1, $2, $3)
		ON CONFLICT ON CONSTRAINT roomserver_user_room_keys_pk DO UPDATE SET pseudo_id_pub_key = $3
		RETURNING (pseudo_id_pub_key)
	`

	// selectUserRoomKeySQL selects a user's private key for a room
	selectUserRoomKeySQL = `SELECT pseudo_id_key FROM roomserver_user_room_keys WHERE user_nid = $1 AND room_nid = $2`

	// selectUserRoomPublicKeySQL selects a user's public key for a room
	selectUserRoomPublicKeySQL = `SELECT pseudo_id_pub_key FROM roomserver_user_room_keys WHERE user_nid = $1 AND room_nid = $2`

	// selectUserNIDsSQL looks up user NIDs by room NIDs and public keys
	selectUserNIDsSQL = `SELECT user_nid, room_nid, pseudo_id_pub_key FROM roomserver_user_room_keys WHERE room_nid = ANY($1) AND pseudo_id_pub_key = ANY($2)`

	// selectAllUserRoomPublicKeyForUserSQL gets all public keys for a user
	selectAllUserRoomPublicKeyForUserSQL = `SELECT room_nid, pseudo_id_pub_key FROM roomserver_user_room_keys WHERE user_nid = $1`
)

type postgresUserRoomKeysTable struct {
	cm *sqlutil.Connections
	
	// SQL statements stored as struct fields
	insertUserRoomPrivateKeyStmt       string
	insertUserRoomPublicKeyStmt        string
	selectUserRoomKeyStmt              string
	selectUserRoomPublicKeyStmt        string
	selectUserNIDsStmt                 string
	selectAllUserRoomPublicKeyForUserStmt string
}

func NewPostgresUserRoomKeysTable(ctx context.Context, cm *sqlutil.Connections) (tables.UserRoomKeys, error) {
	// Create the table first
	db := cm.Connection(ctx, false)
	if err := db.Exec(userRoomKeysSchema).Error; err != nil {
		return nil, err
	}
	
	s := &postgresUserRoomKeysTable{
		cm: cm,
		
		// Initialize SQL statement fields with the constants
		insertUserRoomPrivateKeyStmt:       insertUserRoomPrivateKeySQL,
		insertUserRoomPublicKeyStmt:        insertUserRoomPublicKeySQL,
		selectUserRoomKeyStmt:              selectUserRoomKeySQL,
		selectUserRoomPublicKeyStmt:        selectUserRoomPublicKeySQL,
		selectUserNIDsStmt:                 selectUserNIDsSQL,
		selectAllUserRoomPublicKeyForUserStmt: selectAllUserRoomPublicKeyForUserSQL,
	}
	
	return s, nil
}

func (s *postgresUserRoomKeysTable) InsertUserRoomPrivatePublicKey(
	ctx context.Context, 
	userNID types.EventStateKeyNID, 
	roomNID types.RoomNID, 
	key ed25519.PrivateKey,
) (result ed25519.PrivateKey, err error) {
	db := s.cm.Connection(ctx, false)
	row := db.Raw(
		s.insertUserRoomPrivateKeyStmt,
		userNID, 
		roomNID, 
		key, 
		key.Public(),
	).Row()
	
	err = row.Scan(&result)
	return result, err
}

func (s *postgresUserRoomKeysTable) InsertUserRoomPublicKey(
	ctx context.Context, 
	userNID types.EventStateKeyNID, 
	roomNID types.RoomNID, 
	key ed25519.PublicKey,
) (result ed25519.PublicKey, err error) {
	db := s.cm.Connection(ctx, false)
	row := db.Raw(
		s.insertUserRoomPublicKeyStmt,
		userNID, 
		roomNID, 
		key,
	).Row()
	
	err = row.Scan(&result)
	return result, err
}

func (s *postgresUserRoomKeysTable) SelectUserRoomPrivateKey(
	ctx context.Context,
	userNID types.EventStateKeyNID,
	roomNID types.RoomNID,
) (ed25519.PrivateKey, error) {
	db := s.cm.Connection(ctx, true)
	
	var result ed25519.PrivateKey
	row := db.Raw(
		s.selectUserRoomKeyStmt,
		userNID, 
		roomNID,
	).Row()
	
	err := row.Scan(&result)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return result, err
}

func (s *postgresUserRoomKeysTable) SelectUserRoomPublicKey(
	ctx context.Context,
	userNID types.EventStateKeyNID,
	roomNID types.RoomNID,
) (ed25519.PublicKey, error) {
	db := s.cm.Connection(ctx, true)
	
	var result ed25519.PublicKey
	row := db.Raw(
		s.selectUserRoomPublicKeyStmt,
		userNID, 
		roomNID,
	).Row()
	
	err := row.Scan(&result)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return result, err
}

func (s *postgresUserRoomKeysTable) BulkSelectUserNIDs(
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
	
	rows, err := db.Raw(
		s.selectUserNIDsStmt,
		pq.Array(roomNIDs), 
		pq.Array(senders),
	).Rows()
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

func (s *postgresUserRoomKeysTable) SelectAllPublicKeysForUser(
	ctx context.Context, 
	userNID types.EventStateKeyNID,
) (map[types.RoomNID]ed25519.PublicKey, error) {
	db := s.cm.Connection(ctx, true)
	
	rows, err := db.Raw(
		s.selectAllUserRoomPublicKeyForUserStmt,
		userNID,
	).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "failed to close rows")

	result := make(map[types.RoomNID]ed25519.PublicKey)
	var roomNID types.RoomNID
	var pubKey []byte
	for rows.Next() {
		if err = rows.Scan(&roomNID, &pubKey); err != nil {
			return nil, err
		}
		result[roomNID] = pubKey
	}
	return result, rows.Err()
}
