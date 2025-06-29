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

package routing

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/clientapi/httputil"
	roomserverAPI "github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/syncapi/storage"
	"github.com/antinvestor/matrix/syncapi/synctypes"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/util"
	"github.com/tidwall/gjson"
)

// nolint:gocyclo
func Search(req *http.Request, device *api.Device, syncDB storage.Database, from *string, rsAPI roomserverAPI.SyncRoomserverAPI) util.JSONResponse {
	start := time.Now()
	var (
		searchReq SearchRequest
		err       error
		ctx       = req.Context()
	)
	resErr := httputil.UnmarshalJSONRequest(req, &searchReq)
	if resErr != nil {
		util.Log(ctx).Error("failed to unmarshal search request")
		return *resErr
	}

	nextBatch := 0
	if from != nil && *from != "" {
		nextBatch, err = strconv.Atoi(*from)
		if err != nil {
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
	}

	if searchReq.SearchCategories.RoomEvents.Filter.Limit == 0 {
		searchReq.SearchCategories.RoomEvents.Filter.Limit = 5
	}

	snapshot, err := syncDB.NewDatabaseSnapshot(req.Context())
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// only search rooms the user is actually joined to
	joinedRooms, err := snapshot.RoomIDsWithMembership(ctx, device.UserID, "join")
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	if len(joinedRooms) == 0 {
		return util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("User not joined to any rooms."),
		}
	}
	joinedRoomsMap := make(map[string]struct{}, len(joinedRooms))
	for _, roomID := range joinedRooms {
		joinedRoomsMap[roomID] = struct{}{}
	}
	var rooms []string
	if searchReq.SearchCategories.RoomEvents.Filter.Rooms != nil {
		for _, roomID := range *searchReq.SearchCategories.RoomEvents.Filter.Rooms {
			if _, ok := joinedRoomsMap[roomID]; ok {
				rooms = append(rooms, roomID)
			}
		}
	} else {
		rooms = joinedRooms
	}

	if len(rooms) == 0 {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Unknown("User not allowed to search in this room(s)."),
		}
	}

	result, err := syncDB.SearchEvents(ctx,
		searchReq.SearchCategories.RoomEvents.SearchTerm, rooms,
		searchReq.SearchCategories.RoomEvents.Keys,
		searchReq.SearchCategories.RoomEvents.Filter.Limit, nextBatch,
	)
	if err != nil {
		util.Log(ctx).WithError(err).Error("failed fulltext search")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// From was specified but empty, return no results, only the count
	if from != nil && *from == "" {
		return util.JSONResponse{
			Code: http.StatusOK,
			JSON: SearchResponse{
				SearchCategories: SearchCategoriesResponse{
					RoomEvents: RoomEventsResponse{
						Count:     result.Total,
						NextBatch: nil,
					},
				},
			},
		}
	}

	var results []Result

	// Filter on m.room.message, as otherwise we also get events like m.reaction
	// which "breaks" displaying results in Element Web.
	eventTypes := []string{"m.room.message"}
	roomFilter := &synctypes.RoomEventFilter{
		Rooms: &rooms,
		Types: &eventTypes,
	}

	groups := make(map[string]RoomResult)
	knownUsersProfiles := make(map[string]ProfileInfoResponse)

	// orderByTime := searchReq.SearchCategories.RoomEvents.OrderBy == "recent"

	// Sort the events by depth, as the returned values aren't ordered
	// if orderByTime {
	//	sort.Slice(evs, func(i, j int) bool {
	//		return evs[i].Depth() > evs[j].Depth()
	//	})
	// }

	stateForRooms := make(map[string][]synctypes.ClientEvent)
	for _, hit := range result.Results {
		event := hit.Event
		eventsBefore, eventsAfter, err := contextEvents(ctx, snapshot, event, roomFilter, searchReq)
		if err != nil {
			util.Log(ctx).WithError(err).Error("failed to get context events")
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		startToken, endToken, err := getStartEnd(ctx, snapshot, eventsBefore, eventsAfter)
		if err != nil {
			util.Log(ctx).WithError(err).Error("failed to get start/end")
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}

		profileInfos := make(map[string]ProfileInfoResponse)
		for _, ev := range append(eventsBefore, eventsAfter...) {
			userID, queryErr := rsAPI.QueryUserIDForSender(req.Context(), ev.RoomID(), ev.SenderID())
			if queryErr != nil {
				util.Log(ctx).WithError(queryErr).WithField("sender_id", ev.SenderID()).Warn("failed to query userprofile")
				continue
			}

			profile, ok := knownUsersProfiles[userID.String()]
			if !ok {
				stateEvent, stateErr := snapshot.GetStateEvent(ctx, ev.RoomID().String(), spec.MRoomMember, string(ev.SenderID()))
				if stateErr != nil {
					util.Log(ctx).WithError(stateErr).WithField("sender_id", event.SenderID()).Warn("failed to query userprofile")
					continue
				}
				if stateEvent == nil {
					continue
				}
				profile = ProfileInfoResponse{
					AvatarURL:   gjson.GetBytes(stateEvent.Content(), "avatar_url").Str,
					DisplayName: gjson.GetBytes(stateEvent.Content(), "displayname").Str,
				}
				knownUsersProfiles[userID.String()] = profile
			}
			profileInfos[userID.String()] = profile
		}

		clientEvent, err := synctypes.ToClientEvent(event, synctypes.FormatAll, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
		})
		if err != nil {
			util.Log(req.Context()).WithError(err).WithField("senderID", event.SenderID()).Error("Failed converting to ClientEvent")
			continue
		}

		hitScore := 0.0
		if hit.Score != nil {
			hitScore = *hit.Score
		}

		results = append(results, Result{
			Context: SearchContextResponse{
				Start: startToken.String(),
				End:   endToken.String(),
				EventsAfter: synctypes.ToClientEvents(ctx, gomatrixserverlib.ToPDUs(eventsAfter), synctypes.FormatSync, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
					return rsAPI.QueryUserIDForSender(req.Context(), roomID, senderID)
				}),
				EventsBefore: synctypes.ToClientEvents(ctx, gomatrixserverlib.ToPDUs(eventsBefore), synctypes.FormatSync, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
					return rsAPI.QueryUserIDForSender(req.Context(), roomID, senderID)
				}),
				ProfileInfo: profileInfos,
			},
			Rank:   hitScore,
			Result: *clientEvent,
		})
		roomGroup := groups[event.RoomID().String()]
		roomGroup.Results = append(roomGroup.Results, event.EventID())
		groups[event.RoomID().String()] = roomGroup
		if _, ok := stateForRooms[event.RoomID().String()]; searchReq.SearchCategories.RoomEvents.IncludeState && !ok {
			stateFilter := synctypes.DefaultStateFilter()
			state, err := snapshot.CurrentState(ctx, event.RoomID().String(), &stateFilter, nil)
			if err != nil {
				util.Log(ctx).WithError(err).Error("unable to get current state")
				return util.JSONResponse{
					Code: http.StatusInternalServerError,
					JSON: spec.InternalServerError{},
				}
			}
			stateForRooms[event.RoomID().String()] = synctypes.ToClientEvents(ctx, gomatrixserverlib.ToPDUs(state), synctypes.FormatSync, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
				return rsAPI.QueryUserIDForSender(req.Context(), roomID, senderID)
			})
		}
	}

	var nextBatchResult *string = nil
	if result.Total > nextBatch+len(results) {
		nb := strconv.Itoa(len(results) + nextBatch)
		nextBatchResult = &nb
	} else if result.Total == nextBatch+len(results) {
		// Sytest expects a next_batch even if we don't actually have any more results
		nb := ""
		nextBatchResult = &nb
	}

	res := SearchResponse{
		SearchCategories: SearchCategoriesResponse{
			RoomEvents: RoomEventsResponse{
				Count:      result.Total,
				Groups:     Groups{RoomID: groups},
				Results:    results,
				NextBatch:  nextBatchResult,
				Highlights: result.Highlights(),
				State:      stateForRooms,
			},
		},
	}

	util.Log(ctx).Debug("Full search request took %v", time.Since(start))

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: res,
	}
}

// contextEvents returns the events around a given eventID
func contextEvents(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	event *types.HeaderedEvent,
	roomFilter *synctypes.RoomEventFilter,
	searchReq SearchRequest,
) ([]*types.HeaderedEvent, []*types.HeaderedEvent, error) {
	id, _, err := snapshot.SelectContextEvent(ctx, event.RoomID().String(), event.EventID())
	if err != nil {
		util.Log(ctx).WithError(err).Error("failed to query context event")
		return nil, nil, err
	}
	roomFilter.Limit = searchReq.SearchCategories.RoomEvents.EventContext.BeforeLimit
	eventsBefore, err := snapshot.SelectContextBeforeEvent(ctx, id, event.RoomID().String(), roomFilter)
	if err != nil {
		util.Log(ctx).WithError(err).Error("failed to query before context event")
		return nil, nil, err
	}
	roomFilter.Limit = searchReq.SearchCategories.RoomEvents.EventContext.AfterLimit
	_, eventsAfter, err := snapshot.SelectContextAfterEvent(ctx, id, event.RoomID().String(), roomFilter)
	if err != nil {
		util.Log(ctx).WithError(err).Error("failed to query after context event")
		return nil, nil, err
	}
	return eventsBefore, eventsAfter, err
}

type EventContext struct {
	AfterLimit     int  `json:"after_limit,omitempty"`
	BeforeLimit    int  `json:"before_limit,omitempty"`
	IncludeProfile bool `json:"include_profile,omitempty"`
}

type GroupBy struct {
	Key string `json:"key"`
}

type Groupings struct {
	GroupBy []GroupBy `json:"group_by"`
}

type RoomEvents struct {
	EventContext EventContext              `json:"event_context"`
	Filter       synctypes.RoomEventFilter `json:"filter"`
	Groupings    Groupings                 `json:"groupings"`
	IncludeState bool                      `json:"include_state"`
	Keys         []string                  `json:"keys"`
	OrderBy      string                    `json:"order_by"`
	SearchTerm   string                    `json:"search_term"`
}

type SearchCategories struct {
	RoomEvents RoomEvents `json:"room_events"`
}

type SearchRequest struct {
	SearchCategories SearchCategories `json:"search_categories"`
}

type SearchResponse struct {
	SearchCategories SearchCategoriesResponse `json:"search_categories"`
}
type RoomResult struct {
	NextBatch *string  `json:"next_batch,omitempty"`
	Order     int      `json:"order"`
	Results   []string `json:"results"`
}

type Groups struct {
	RoomID map[string]RoomResult `json:"room_id"`
}

type Result struct {
	Context SearchContextResponse `json:"context"`
	Rank    float64               `json:"rank"`
	Result  synctypes.ClientEvent `json:"result"`
}

type SearchContextResponse struct {
	End          string                         `json:"end"`
	EventsAfter  []synctypes.ClientEvent        `json:"events_after"`
	EventsBefore []synctypes.ClientEvent        `json:"events_before"`
	Start        string                         `json:"start"`
	ProfileInfo  map[string]ProfileInfoResponse `json:"profile_info"`
}

type ProfileInfoResponse struct {
	AvatarURL   string `json:"avatar_url"`
	DisplayName string `json:"display_name"`
}

type RoomEventsResponse struct {
	Count      int                                `json:"count"`
	Groups     Groups                             `json:"groups"`
	Highlights []string                           `json:"highlights"`
	NextBatch  *string                            `json:"next_batch,omitempty"`
	Results    []Result                           `json:"results"`
	State      map[string][]synctypes.ClientEvent `json:"state,omitempty"`
}
type SearchCategoriesResponse struct {
	RoomEvents RoomEventsResponse `json:"room_events"`
}
