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

package routing

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/clientapi/httputil"
	"github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/setup/config"
	userapi "github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/util"
)

type invite struct {
	MXID   string                                         `json:"mxid"`
	RoomID string                                         `json:"room_id"`
	Sender string                                         `json:"sender"`
	Token  string                                         `json:"token"`
	Signed gomatrixserverlib.MemberThirdPartyInviteSigned `json:"signed"`
}

type invites struct {
	Medium  string   `json:"medium"`
	Address string   `json:"address"`
	MXID    string   `json:"mxid"`
	Invites []invite `json:"invites"`
}

var (
	errNotLocalUser = errors.New("the user is not from this server")
	errNotInRoom    = errors.New("the server isn't currently in the room")
)

// CreateInvitesFrom3PIDInvites implements POST /_matrix/federation/v1/3pid/onbind
func CreateInvitesFrom3PIDInvites(
	req *http.Request, rsAPI api.FederationRoomserverAPI,
	cfg *config.FederationAPI,
	federation fclient.FederationClient,
	userAPI userapi.FederationUserAPI,
) util.JSONResponse {
	var body invites
	if reqErr := httputil.UnmarshalJSONRequest(req, &body); reqErr != nil {
		return *reqErr
	}

	evs := []*types.HeaderedEvent{}
	for _, inv := range body.Invites {
		_, err := rsAPI.QueryRoomVersionForRoom(req.Context(), inv.RoomID)
		if err != nil {
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.UnsupportedRoomVersion(err.Error()),
			}
		}

		event, err := createInviteFrom3PIDInvite(
			req.Context(), rsAPI, cfg, inv, federation, userAPI,
		)
		if err != nil {
			util.Log(req.Context()).WithError(err).Error("createInviteFrom3PIDInvite failed")
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		if event != nil {
			evs = append(evs, &types.HeaderedEvent{PDU: event})
		}
	}

	// Send all the events
	if err := api.SendEvents(
		req.Context(),
		rsAPI,
		api.KindNew,
		evs,
		cfg.Global.ServerName, // TODO: which virtual host?
		"TODO",
		cfg.Global.ServerName,
		nil,
		false,
	); err != nil {
		util.Log(req.Context()).WithError(err).Error("SendEvents failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}

// ExchangeThirdPartyInvite implements PUT /_matrix/federation/v1/exchange_third_party_invite/{roomID}
func ExchangeThirdPartyInvite(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	roomID string,
	rsAPI api.FederationRoomserverAPI,
	cfg *config.FederationAPI,
	federation fclient.FederationClient,
) util.JSONResponse {
	var proto gomatrixserverlib.ProtoEvent
	if err := json.Unmarshal(request.Content(), &proto); err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.NotJSON("The request body could not be decoded into valid JSON. " + err.Error()),
		}
	}

	// Check that the room ID is correct.
	if proto.RoomID != roomID {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The room ID in the request path must match the room ID in the invite event JSON"),
		}
	}

	validRoomID, err := spec.NewRoomID(roomID)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("Invalid room ID"),
		}
	}
	userID, err := rsAPI.QueryUserIDForSender(httpReq.Context(), *validRoomID, spec.SenderID(proto.SenderID))
	if err != nil || userID == nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("Invalid sender ID"),
		}
	}
	senderDomain := userID.Domain()

	// Check that the state key is correct.
	targetUserID, err := rsAPI.QueryUserIDForSender(httpReq.Context(), *validRoomID, spec.SenderID(*proto.StateKey))
	if err != nil || targetUserID == nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The event's state key isn't a Global user ID"),
		}
	}
	targetDomain := targetUserID.Domain()

	// Check that the target user is from the requesting homeserver.
	if targetDomain != request.Origin() {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The event's state key doesn't have the same domain as the request's origin"),
		}
	}

	roomVersion, err := rsAPI.QueryRoomVersionForRoom(httpReq.Context(), roomID)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.UnsupportedRoomVersion(err.Error()),
		}
	}

	// Auth and build the event from what the remote server sent us
	event, err := buildMembershipEvent(httpReq.Context(), &proto, rsAPI, cfg)
	if errors.Is(err, errNotInRoom) {
		return util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("Unknown room " + roomID),
		}
	} else if err != nil {
		util.Log(httpReq.Context()).WithError(err).Error("buildMembershipEvent failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// Ask the requesting server to sign the newly created event so we know it
	// acknowledged it
	inviteReq, err := fclient.NewInviteV2Request(event, nil)
	if err != nil {
		util.Log(httpReq.Context()).WithError(err).Error("failed to make invite v2 request")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	signedEvent, err := federation.SendInviteV2(httpReq.Context(), senderDomain, request.Origin(), inviteReq)
	if err != nil {
		util.Log(httpReq.Context()).WithError(err).Error("federation.SendInvite failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	verImpl, err := gomatrixserverlib.GetRoomVersion(roomVersion)
	if err != nil {
		util.Log(httpReq.Context()).WithError(err).WithField("room_version", roomVersion).Error("Unknown room version")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	inviteEvent, err := verImpl.NewEventFromUntrustedJSON(signedEvent.Event)
	if err != nil {
		util.Log(httpReq.Context()).WithError(err).Error("federation.SendInvite failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// Send the event to the Roomserver
	if err = api.SendEvents(
		httpReq.Context(), rsAPI,
		api.KindNew,
		[]*types.HeaderedEvent{
			{PDU: inviteEvent},
		},
		request.Destination(),
		request.Origin(),
		cfg.Global.ServerName,
		nil,
		false,
	); err != nil {
		util.Log(httpReq.Context()).WithError(err).Error("SendEvents failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}

// createInviteFrom3PIDInvite processes an invite provided by the identity server
// and creates a m.room.member event (with "invite" membership) from it.
// Returns an error if there was a problem building the event or fetching the
// necessary data to do so.
func createInviteFrom3PIDInvite(
	ctx context.Context, rsAPI api.FederationRoomserverAPI,
	cfg *config.FederationAPI,
	inv invite, federation fclient.FederationClient,
	userAPI userapi.FederationUserAPI,
) (gomatrixserverlib.PDU, error) {
	_, server, err := gomatrixserverlib.SplitID('@', inv.MXID)
	if err != nil {
		return nil, err
	}

	if server != cfg.Global.ServerName {
		return nil, errNotLocalUser
	}

	// Build the event
	proto := &gomatrixserverlib.ProtoEvent{
		Type:     "m.room.member",
		SenderID: inv.Sender,
		RoomID:   inv.RoomID,
		StateKey: &inv.MXID,
	}

	profile, err := userAPI.QueryProfile(ctx, inv.MXID)
	if err != nil {
		return nil, err
	}

	content := gomatrixserverlib.MemberContent{
		AvatarURL:   profile.AvatarURL,
		DisplayName: profile.DisplayName,
		Membership:  spec.Invite,
		ThirdPartyInvite: &gomatrixserverlib.MemberThirdPartyInvite{
			Signed: inv.Signed,
		},
	}

	if err = proto.SetContent(content); err != nil {
		return nil, err
	}

	event, err := buildMembershipEvent(ctx, proto, rsAPI, cfg)
	if errors.Is(err, errNotInRoom) {
		return nil, sendToRemoteServer(ctx, inv, federation, cfg, *proto)
	}
	if err != nil {
		return nil, err
	}

	return event, nil
}

// buildMembershipEvent uses a builder for a m.room.member invite event derived
// from a third-party invite to auth and build the said event. Returns the said
// event.
// Returns errNotInRoom if the server is not in the room the invite is for.
// Returns an error if something failed during the process.
func buildMembershipEvent(
	ctx context.Context,
	protoEvent *gomatrixserverlib.ProtoEvent, rsAPI api.FederationRoomserverAPI,
	cfg *config.FederationAPI,
) (gomatrixserverlib.PDU, error) {
	eventsNeeded, err := gomatrixserverlib.StateNeededForProtoEvent(protoEvent)
	if err != nil {
		return nil, err
	}

	if len(eventsNeeded.Tuples()) == 0 {
		return nil, errors.New("expecting state tuples for event builder, got none")
	}

	// Ask the Roomserver for information about this room
	queryReq := api.QueryLatestEventsAndStateRequest{
		RoomID:       protoEvent.RoomID,
		StateToFetch: eventsNeeded.Tuples(),
	}
	var queryRes api.QueryLatestEventsAndStateResponse
	if err = rsAPI.QueryLatestEventsAndState(ctx, &queryReq, &queryRes); err != nil {
		return nil, err
	}

	if !queryRes.RoomExists {
		// Use federation to auth the event
		return nil, errNotInRoom
	}

	// Auth the event locally
	protoEvent.Depth = queryRes.Depth
	protoEvent.PrevEvents = queryRes.LatestEvents

	authEvents, err := gomatrixserverlib.NewAuthEvents(nil)
	if err != nil {
		return nil, err
	}

	for i := range queryRes.StateEvents {
		err = authEvents.AddEvent(queryRes.StateEvents[i].PDU)
		if err != nil {
			return nil, err
		}
	}

	if err = fillDisplayName(protoEvent, authEvents); err != nil {
		return nil, err
	}

	refs, err := eventsNeeded.AuthEventReferences(authEvents)
	if err != nil {
		return nil, err
	}
	protoEvent.AuthEvents = refs

	verImpl, err := gomatrixserverlib.GetRoomVersion(queryRes.RoomVersion)
	if err != nil {
		return nil, err
	}
	builder := verImpl.NewEventBuilderFromProtoEvent(protoEvent)

	event, err := builder.Build(
		time.Now(), cfg.Global.ServerName, cfg.Global.KeyID,
		cfg.Global.PrivateKey,
	)

	return event, err
}

// sendToRemoteServer uses federation to send an invite provided by an identity
// server to a remote server in case the current server isn't in the room the
// invite is for.
// Returns an error if it couldn't get the server names to reach or if all of
// them responded with an error.
func sendToRemoteServer(
	ctx context.Context, inv invite,
	federation fclient.FederationClient, cfg *config.FederationAPI,
	proto gomatrixserverlib.ProtoEvent,
) (err error) {

	log := util.Log(ctx)
	remoteServers := make([]spec.ServerName, 2)
	_, remoteServers[0], err = gomatrixserverlib.SplitID('@', inv.Sender)
	if err != nil {
		return
	}
	// Fallback to the room's server if the sender's domain is the same as
	// the current server's
	_, remoteServers[1], err = gomatrixserverlib.SplitID('!', inv.RoomID)
	if err != nil {
		return
	}

	for _, server := range remoteServers {
		err = federation.ExchangeThirdPartyInvite(ctx, cfg.Global.ServerName, server, proto)
		if err == nil {
			return
		}
		log.WithError(err).WithField("server", server).Warn("Failed to send 3PID invite via server")
	}

	return errors.New("failed to send 3PID invite via any server")
}

// fillDisplayName looks in a list of auth events for a m.room.third_party_invite
// event with the state key matching a given m.room.member event's content's token.
// If such an event is found, fills the "display_name" attribute of the
// "third_party_invite" structure in the m.room.member event with the display_name
// from the m.room.third_party_invite event.
// Returns an error if there was a problem parsing the m.room.third_party_invite
// event's content or updating the m.room.member event's content.
// Returns nil if no m.room.third_party_invite with a matching token could be
// found. Returning an error isn't necessary in this case as the event will be
// rejected by gomatrixserverlib.
func fillDisplayName(
	builder *gomatrixserverlib.ProtoEvent, authEvents *gomatrixserverlib.AuthEvents,
) error {
	var content gomatrixserverlib.MemberContent
	if err := json.Unmarshal(builder.Content, &content); err != nil {
		return err
	}

	// Look for the m.room.third_party_invite event
	thirdPartyInviteEvent, _ := authEvents.ThirdPartyInvite(content.ThirdPartyInvite.Signed.Token)

	if thirdPartyInviteEvent == nil {
		// If the third party invite event doesn't exist then we can't use it to set the display name.
		return nil
	}

	var thirdPartyInviteContent gomatrixserverlib.ThirdPartyInviteContent
	if err := json.Unmarshal(thirdPartyInviteEvent.Content(), &thirdPartyInviteContent); err != nil {
		return err
	}

	// Use the m.room.third_party_invite event to fill the "displayname" and
	// update the m.room.member event's content with it
	content.ThirdPartyInvite.DisplayName = thirdPartyInviteContent.DisplayName
	return builder.SetContent(content)
}
