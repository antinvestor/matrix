package routing

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/clientapi/httputil"
	roomserverAPI "github.com/antinvestor/matrix/roomserver/api"
	"github.com/pitabwire/util"
)

type PublicRoomReq struct {
	Since              string `json:"since,omitempty"`
	Limit              int16  `json:"limit,omitempty"`
	Filter             filter `json:"filter,omitempty"`
	IncludeAllNetworks bool   `json:"include_all_networks,omitempty"`
	NetworkID          string `json:"third_party_instance_id,omitempty"`
}

type filter struct {
	SearchTerms string   `json:"generic_search_term,omitempty"`
	RoomTypes   []string `json:"room_types,omitempty"`
}

// GetPostPublicRooms implements GET and POST /publicRooms
func GetPostPublicRooms(req *http.Request, rsAPI roomserverAPI.FederationRoomserverAPI) util.JSONResponse {
	var request PublicRoomReq
	if fillErr := fillPublicRoomsReq(req, &request); fillErr != nil {
		return *fillErr
	}
	if request.Limit == 0 {
		request.Limit = 50
	}
	response, err := publicRooms(req.Context(), request, rsAPI)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: response,
	}
}

func publicRooms(
	ctx context.Context, request PublicRoomReq, rsAPI roomserverAPI.FederationRoomserverAPI,
) (*fclient.RespPublicRooms, error) {

	var response fclient.RespPublicRooms
	var limit int16
	var offset int64
	limit = request.Limit
	offset, err := strconv.ParseInt(request.Since, 10, 64)
	// ParseInt returns 0 and an error when trying to parse an empty string
	// In that case, we want to assign 0 so we ignore the error
	if err != nil && len(request.Since) > 0 {
		util.Log(ctx).WithError(err).Error("strconv.ParseInt failed")
		return nil, err
	}

	if request.IncludeAllNetworks && request.NetworkID != "" {
		return nil, fmt.Errorf("include_all_networks and third_party_instance_id can not be used together")
	}

	var queryRes roomserverAPI.QueryPublishedRoomsResponse
	err = rsAPI.QueryPublishedRooms(ctx, &roomserverAPI.QueryPublishedRoomsRequest{
		NetworkID: request.NetworkID,
	}, &queryRes)
	if err != nil {
		util.Log(ctx).WithError(err).Error("QueryPublishedRooms failed")
		return nil, err
	}
	response.TotalRoomCountEstimate = len(queryRes.RoomIDs)

	if offset > 0 {
		response.PrevBatch = strconv.Itoa(int(offset) - 1)
	}
	nextIndex := int(offset) + int(limit)
	if response.TotalRoomCountEstimate > nextIndex {
		response.NextBatch = strconv.Itoa(nextIndex)
	}

	if offset < 0 {
		offset = 0
	}
	if nextIndex > len(queryRes.RoomIDs) {
		nextIndex = len(queryRes.RoomIDs)
	}
	roomIDs := queryRes.RoomIDs[offset:nextIndex]
	response.Chunk, err = fillInRooms(ctx, roomIDs, rsAPI)
	return &response, err
}

// fillPublicRoomsReq fills the Limit, Since and Filter attributes of a GET or POST request
// on /publicRooms by parsing the incoming HTTP request
// Filter is only filled for POST requests
func fillPublicRoomsReq(httpReq *http.Request, request *PublicRoomReq) *util.JSONResponse {
	switch httpReq.Method {
	case http.MethodGet:
		limit, err := strconv.Atoi(httpReq.FormValue("limit"))
		// Atoi returns 0 and an error when trying to parse an empty string
		// In that case, we want to assign 0 so we ignore the error
		if err != nil && len(httpReq.FormValue("limit")) > 0 {
			util.Log(httpReq.Context()).WithError(err).Error("strconv.Atoi failed")
			return &util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		request.Limit = int16(limit)
		request.Since = httpReq.FormValue("since")
		return nil
	case http.MethodPost:
		return httputil.UnmarshalJSONRequest(httpReq, request)
	}

	return &util.JSONResponse{
		Code: http.StatusMethodNotAllowed,
		JSON: spec.NotFound("Bad method"),
	}
}

// due to lots of switches
func fillInRooms(ctx context.Context, roomIDs []string, rsAPI roomserverAPI.FederationRoomserverAPI) ([]fclient.PublicRoom, error) {
	avatarTuple := gomatrixserverlib.StateKeyTuple{EventType: "m.room.avatar", StateKey: ""}
	nameTuple := gomatrixserverlib.StateKeyTuple{EventType: "m.room.name", StateKey: ""}
	canonicalTuple := gomatrixserverlib.StateKeyTuple{EventType: spec.MRoomCanonicalAlias, StateKey: ""}
	topicTuple := gomatrixserverlib.StateKeyTuple{EventType: "m.room.topic", StateKey: ""}
	guestTuple := gomatrixserverlib.StateKeyTuple{EventType: "m.room.guest_access", StateKey: ""}
	visibilityTuple := gomatrixserverlib.StateKeyTuple{EventType: spec.MRoomHistoryVisibility, StateKey: ""}
	joinRuleTuple := gomatrixserverlib.StateKeyTuple{EventType: spec.MRoomJoinRules, StateKey: ""}

	var stateRes roomserverAPI.QueryBulkStateContentResponse
	err := rsAPI.QueryBulkStateContent(ctx, &roomserverAPI.QueryBulkStateContentRequest{
		RoomIDs:        roomIDs,
		AllowWildcards: true,
		StateTuples: []gomatrixserverlib.StateKeyTuple{
			nameTuple, canonicalTuple, topicTuple, guestTuple, visibilityTuple, joinRuleTuple, avatarTuple,
			{EventType: spec.MRoomMember, StateKey: "*"},
		},
	}, &stateRes)
	if err != nil {
		util.Log(ctx).WithError(err).Error("QueryBulkStateContent failed")
		return nil, err
	}
	chunk := make([]fclient.PublicRoom, len(roomIDs))
	i := 0
	for roomID, data := range stateRes.Rooms {
		pub := fclient.PublicRoom{
			RoomID: roomID,
		}
		joinCount := 0
		var joinRule, guestAccess string
		for tuple, contentVal := range data {
			if tuple.EventType == spec.MRoomMember && contentVal == "join" {
				joinCount++
				continue
			}
			switch tuple {
			case avatarTuple:
				pub.AvatarURL = contentVal
			case nameTuple:
				pub.Name = contentVal
			case topicTuple:
				pub.Topic = contentVal
			case canonicalTuple:
				if _, _, err := gomatrixserverlib.SplitID('#', contentVal); err == nil {
					pub.CanonicalAlias = contentVal
				}
			case visibilityTuple:
				pub.WorldReadable = contentVal == "world_readable"
			// need both of these to determine whether guests can join
			case joinRuleTuple:
				joinRule = contentVal
			case guestTuple:
				guestAccess = contentVal
			}
		}
		if joinRule == spec.Public && guestAccess == "can_join" {
			pub.GuestCanJoin = true
		}
		pub.JoinedMembersCount = joinCount
		chunk[i] = pub
		i++
	}
	return chunk, nil
}
