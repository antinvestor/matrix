// Copyright 2020 New Vector Ltd
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

package perform

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	fsAPI "github.com/antinvestor/matrix/federationapi/api"
	"github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/roomserver/internal/input"
	"github.com/antinvestor/matrix/roomserver/storage"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/util"
)

type Peeker struct {
	ServerName spec.ServerName
	Cfg        *config.RoomServer
	FSAPI      fsAPI.RoomserverFederationAPI
	DB         storage.Database

	Inputer *input.Inputer
}

// PerformPeek handles peeking into matrix rooms, including over federation by talking to the federationapi.
func (r *Peeker) PerformPeek(
	ctx context.Context,
	req *api.PerformPeekRequest,
) (roomID string, err error) {
	return r.performPeek(ctx, req)
}

func (r *Peeker) performPeek(
	ctx context.Context,
	req *api.PerformPeekRequest,
) (string, error) {
	// FIXME: there's way too much duplication with performJoin
	_, domain, err := gomatrixserverlib.SplitID('@', req.UserID)
	if err != nil {
		return "", api.ErrInvalidID{Err: fmt.Errorf("supplied user ID %q in incorrect format", req.UserID)}
	}
	if !r.Cfg.Global.IsLocalServerName(domain) {
		return "", api.ErrInvalidID{Err: fmt.Errorf("user %q does not belong to this homeserver", req.UserID)}
	}
	if strings.HasPrefix(req.RoomIDOrAlias, "!") {
		return r.performPeekRoomByID(ctx, req)
	}
	if strings.HasPrefix(req.RoomIDOrAlias, "#") {
		return r.performPeekRoomByAlias(ctx, req)
	}
	return "", api.ErrInvalidID{Err: fmt.Errorf("room ID or alias %q is invalid", req.RoomIDOrAlias)}
}

func (r *Peeker) performPeekRoomByAlias(
	ctx context.Context,
	req *api.PerformPeekRequest,
) (string, error) {
	// Get the domain part of the room alias.
	_, domain, err := gomatrixserverlib.SplitID('#', req.RoomIDOrAlias)
	if err != nil {
		return "", api.ErrInvalidID{Err: fmt.Errorf("alias %q is not in the correct format", req.RoomIDOrAlias)}
	}
	req.ServerNames = append(req.ServerNames, domain)

	// Check if this alias matches our own server configuration. If it
	// doesn't then we'll need to try a federated peek.
	var roomID string
	if !r.Cfg.Global.IsLocalServerName(domain) {
		// The alias isn't owned by us, so we will need to try peeking using
		// a remote server.
		dirReq := fsAPI.PerformDirectoryLookupRequest{
			RoomAlias:  req.RoomIDOrAlias, // the room alias to lookup
			ServerName: domain,            // the server to ask
		}
		dirRes := fsAPI.PerformDirectoryLookupResponse{}
		err = r.FSAPI.PerformDirectoryLookup(ctx, &dirReq, &dirRes)
		if err != nil {
			util.Log(ctx).WithError(err).Error("error looking up alias %q", req.RoomIDOrAlias)
			return "", fmt.Errorf("looking up alias %q over federation failed: %w", req.RoomIDOrAlias, err)
		}
		roomID = dirRes.RoomID
		req.ServerNames = append(req.ServerNames, dirRes.ServerNames...)
	} else {
		// Otherwise, look up if we know this room alias locally.
		roomID, err = r.DB.GetRoomIDForAlias(ctx, req.RoomIDOrAlias)
		if err != nil {
			return "", fmt.Errorf("lookup room alias %q failed: %w", req.RoomIDOrAlias, err)
		}
	}

	// If the room ID is empty then we failed to look up the alias.
	if roomID == "" {
		return "", fmt.Errorf("alias %q not found", req.RoomIDOrAlias)
	}

	// If we do, then pluck out the room ID and continue the peek.
	req.RoomIDOrAlias = roomID
	return r.performPeekRoomByID(ctx, req)
}

func (r *Peeker) performPeekRoomByID(
	ctx context.Context,
	req *api.PerformPeekRequest,
) (string, error) {

	roomID, err := spec.NewRoomID(req.RoomIDOrAlias)
	if err != nil {
		return "", api.ErrInvalidID{Err: fmt.Errorf("room ID %q is invalid: %w", roomID, err)}
	}
	// Get the domain part of the room ID.
	domain := roomID.Domain()

	// handle federated peeks
	// FIXME: don't create an outbound peek if we already have one going.
	if !r.Cfg.Global.IsLocalServerName(domain) {
		// If the server name in the room ID isn't ours then it's a
		// possible candidate for finding the room via federation. Add
		// it to the list of servers to try.
		req.ServerNames = append(req.ServerNames, domain)

		// Try peeking by all of the supplied server names.
		fedReq := fsAPI.PerformOutboundPeekRequest{
			RoomID:      req.RoomIDOrAlias, // the room ID to try and peek
			ServerNames: req.ServerNames,   // the servers to try peeking via
		}
		fedRes := fsAPI.PerformOutboundPeekResponse{}
		_ = r.FSAPI.PerformOutboundPeek(ctx, &fedReq, &fedRes)
		if fedRes.LastError != nil {
			return "", fedRes.LastError
		}
	}

	// If this room isn't world_readable, we reject.
	// XXX: would be nicer to call this with NIDs
	// XXX: we should probably factor out history_visibility checks into a common utility method somewhere
	// which handles the default value etc.
	var worldReadable = false
	if ev, _ := r.DB.GetStateEvent(ctx, roomID.String(), "m.room.history_visibility", ""); ev != nil {
		content := map[string]string{}
		if err = json.Unmarshal(ev.Content(), &content); err != nil {
			util.Log(ctx).WithError(err).Error("json.Unmarshal for history visibility failed")
			return "", err
		}
		if visibility, ok := content["history_visibility"]; ok {
			worldReadable = visibility == "world_readable"
		}
	}

	if !worldReadable {
		return "", api.ErrNotAllowed{Err: fmt.Errorf("room is not world-readable")}
	}

	if ev, _ := r.DB.GetStateEvent(ctx, roomID.String(), "m.room.encryption", ""); ev != nil {
		return "", api.ErrNotAllowed{Err: fmt.Errorf("cannot peek into an encrypted room")}
	}

	// TODO: handle federated peeks

	err = r.Inputer.OutputProducer.ProduceRoomEvents(ctx, roomID, []api.OutputEvent{
		{
			Type: api.OutputTypeNewPeek,
			NewPeek: &api.OutputNewPeek{
				RoomID:   roomID.String(),
				UserID:   req.UserID,
				DeviceID: req.DeviceID,
			},
		},
	})
	if err != nil {
		return "", err
	}

	// By this point, if req.RoomIDOrAlias contained an alias, then
	// it will have been overwritten with a room ID by performPeekRoomByAlias.
	// We should now include this in the response so that the CS API can
	// return the right room ID.
	return roomID.String(), nil
}
