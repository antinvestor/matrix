// Copyright 2018 Vector Creations Ltd
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

package consumers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	commonv1 "github.com/antinvestor/apis/go/common/v1"
	notificationv1 "github.com/antinvestor/apis/go/notification/v1"
	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/constants"
	"github.com/antinvestor/matrix/syncapi/synctypes"
	"github.com/pitabwire/frame"
	"github.com/pitabwire/util"
)

// ApplicationServiceTransaction is the transaction that is sent off to an
// application service.
type ApplicationServiceTransaction struct {
	Events []synctypes.ClientEvent `json:"events"`
}

type appserviceState struct {
	*config.ApplicationService
	backoff int

	handler *OutputRoomEventConsumer
}

// OutputRoomEventConsumer consumes events that originated in the room server.
type OutputRoomEventConsumer struct {
	cfg             *config.AppServiceAPI
	qm              queueutil.QueueManager
	rsAPI           api.AppserviceRoomserverAPI
	appServiceMap   map[string]*appserviceState
	notificationCli *notificationv1.NotificationClient
}

// NewOutputRoomEventConsumer creates a new OutputRoomEventConsumer. Call
// Start() to begin consuming from room servers.
func NewOutputRoomEventConsumer(
	ctx context.Context,
	cfg *config.AppServiceAPI,
	qm queueutil.QueueManager,
	rsAPI api.AppserviceRoomserverAPI,
	notificationCli *notificationv1.NotificationClient,
) error {
	c := &OutputRoomEventConsumer{
		cfg:             cfg,
		qm:              qm,
		rsAPI:           rsAPI,
		notificationCli: notificationCli,
	}

	return c.Start(ctx)
}

// Start consuming from room servers
func (s *OutputRoomEventConsumer) Start(ctx context.Context) error {

	s.appServiceMap = make(map[string]*appserviceState)
	for _, as := range s.cfg.Derived.ApplicationServices {
		token := queueutil.Tokenise(as.ID)
		appSvc := &appserviceState{ApplicationService: &as, backoff: 0, handler: s}

		cfg := s.cfg.Queues.OutputAppserviceEvent
		cfg.QReference = fmt.Sprintf("%s_%s", cfg.QReference, token)
		cfg.DS = cfg.DS.ExtendQuery("consumer_durable_name", fmt.Sprintf("%s_%s", cfg.DS.GetQuery("consumer_durable_name"), token))

		err := s.qm.RegisterSubscriber(ctx, &cfg, appSvc)
		if err != nil {
			return err
		}

		s.appServiceMap[token] = appSvc
	}

	return nil
}

// Handle is called when the appservice component receives a new event from
// the room server output log.
func (s *appserviceState) Handle(
	ctx context.Context, metadata map[string]string, message []byte,
) error {

	claims := frame.ClaimsFromContext(ctx)

	util.Log(ctx).WithField("claims", claims).Info("  ------------------- Check received claims")

	return s.handler.filterEventsForAppservice(ctx, s, metadata, message)
}

// filterEventsForAppservice is called when the appservice component receives a new event from
// the room server output log.
func (s *OutputRoomEventConsumer) filterEventsForAppservice(
	ctx context.Context, state *appserviceState, metadata map[string]string, message []byte,
) error {
	logger := util.Log(ctx)

	events := make([]*types.HeaderedEvent, 0, 1)

	// Only handle events we care about
	receivedType := api.OutputType(metadata[constants.RoomEventType])
	if receivedType != api.OutputTypeNewRoomEvent && receivedType != api.OutputTypeNewInviteEvent {
		return nil
	}
	// Parse out the event JSON
	var output api.OutputEvent
	if err := json.Unmarshal(message, &output); err != nil {
		// If the message was invalid, log it and move on to the next message in the stream
		logger.WithField("appservice", state.ID).WithError(err).Error("Appservice failed to parse message, ignoring")
		return nil
	}
	switch output.Type {
	case api.OutputTypeNewRoomEvent:
		if output.NewRoomEvent == nil || !s.appserviceIsInterestedInEvent(ctx, output.NewRoomEvent.Event, state.ApplicationService) {
			return nil
		}
		events = append(events, output.NewRoomEvent.Event)
		if len(output.NewRoomEvent.AddsStateEventIDs) > 0 {
			newEventID := output.NewRoomEvent.Event.EventID()
			eventsReq := &api.QueryEventsByIDRequest{
				RoomID:   output.NewRoomEvent.Event.RoomID().String(),
				EventIDs: make([]string, 0, len(output.NewRoomEvent.AddsStateEventIDs)),
			}
			eventsRes := &api.QueryEventsByIDResponse{}
			for _, eventID := range output.NewRoomEvent.AddsStateEventIDs {
				if eventID != newEventID {
					eventsReq.EventIDs = append(eventsReq.EventIDs, eventID)
				}
			}
			if len(eventsReq.EventIDs) > 0 {
				if err := s.rsAPI.QueryEventsByID(ctx, eventsReq, eventsRes); err != nil {
					logger.WithError(err).Error("s.rsAPI.QueryEventsByID failed")
					return err
				}
				events = append(events, eventsRes.Events...)
			}
		}

	default:
		return nil
	}

	// If there are no events selected for sending then we should
	// ack the messages so that we don't get sent them again in the
	// future.
	if len(events) == 0 {
		return nil
	}

	// Send event to any relevant application services. If we hit
	// an error here, return false, so that we negatively ack.
	if state.IsDistributed {
		return s.sendDistributedEvents(ctx, state, events)
	} else {
		return s.sendEvents(ctx, state, events)
	}
}

// sendEvents passes events to the appservice by using the transactions
// endpoint. It will block for the backoff period if necessary.
func (s *OutputRoomEventConsumer) sendEvents(
	ctx context.Context, state *appserviceState,
	events []*types.HeaderedEvent,
) error {
	// Create the transaction body.
	transaction, err := json.Marshal(
		ApplicationServiceTransaction{
			Events: synctypes.ToClientEvents(ctx, gomatrixserverlib.ToPDUs(events), synctypes.FormatAll, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
				return s.rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
			}),
		},
	)
	if err != nil {
		return err
	}

	// TODO: Switch to something stable. Try to get the message metadata, if we're able to, use the timestamp as the txnID
	txnID := fmt.Sprintf("%d_%d", events[0].OriginServerTS(), len(events))

	// Send the transaction to the appservice.
	// https://spec.matrix.org/v1.9/application-service-api/#pushing-events
	path := "_matrix/app/v1/transactions"
	if s.cfg.LegacyPaths {
		path = "transactions"
	}
	address := fmt.Sprintf("%s/%s/%s", state.RequestUrl(), path, txnID)
	if s.cfg.LegacyAuth {
		address += "?access_token=" + url.QueryEscape(state.HSToken)
	}
	req, err := http.NewRequestWithContext(ctx, "PUT", address, bytes.NewBuffer(transaction))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", state.HSToken))
	resp, err := state.HTTPClient.Do(req)
	if err != nil {
		return state.backoffAndPause(ctx, err)
	}

	// If the response was fine then we can clear any backoffs in place and
	// report that everything was OK. Otherwise, back off for a while.
	switch resp.StatusCode {
	case http.StatusOK:
		state.backoff = 0
	default:
		return state.backoffAndPause(ctx, fmt.Errorf("received HTTP status code %d from appservice url %s", resp.StatusCode, address))
	}
	return nil
}

func (s *OutputRoomEventConsumer) toNotification(ctx context.Context, event synctypes.ClientEvent) *notificationv1.Notification {

	roomProfileID := ""
	roomID, err := spec.NewRoomID(event.RoomID)
	if err == nil {
		roomProfileID = roomID.OpaqueID()
	}

	language := ""
	languages := frame.LanguageFromContext(ctx)
	if languages != nil {
		language = strings.Join(languages, ",")
	}

	profileID := ""
	contactID := ""

	sender := event.SenderKey
	if sender != "" {
		userID := sender.ToUserID()
		profileID = userID.Local()
	}

	claims := frame.ClaimsFromContext(ctx)
	if claims != nil {

		if profileID != "" && claims.Subject != "" {
			if profileID != claims.Subject {
util.Log(ctx).WithField("claim_subject", claims.Subject).WithField("profile_id", profileID).Error("Profile ID from event sender does not match subject in claims")
			}
		}
		contactID = claims.GetContactID()
	}

	data := ""
	var payload map[string]string
	err = json.Unmarshal(event.Content, &payload)
	if err != nil {
		payload = map[string]string{}
		data = string(event.Content)
	}

	return &notificationv1.Notification{
		Id: payload[constants.IDKey],
		Source: &commonv1.ContactLink{
			ProfileId: profileID,
			ContactId: contactID,
		},
		Recipient: &commonv1.ContactLink{
			ProfileId: roomProfileID,
		},
		Type:     "json",
		Payload:  payload,
		Data:     data,
		Language: language,
	}
}

// sendDistributedEvents passes events to the appservice by using the transactions
// endpoint. It will block for the backoff period if necessary.
func (s *OutputRoomEventConsumer) sendDistributedEvents(
	ctx context.Context, _ *appserviceState,
	events []*types.HeaderedEvent,
) error {
	// Create the transaction body.
	clientEvents := synctypes.ToClientEvents(ctx, gomatrixserverlib.ToPDUs(events), synctypes.FormatAll, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
		return s.rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
	})

	notificationList := make([]*notificationv1.Notification, 0, len(events))

	for _, cl := range clientEvents {
		notificationList = append(notificationList, s.toNotification(ctx, cl))
	}

	responsesChan, err := s.notificationCli.Receive(ctx, notificationList)
	if err != nil {
		return err
	}

	for response := range responsesChan {
		if response.Error != nil && response.Error != io.EOF {
			return response.Error
		}
	}

	return nil
}

// backoff pauses the calling goroutine for a 2^some backoff exponent seconds
func (s *appserviceState) backoffAndPause(ctx context.Context, err error) error {
	logger := util.Log(ctx)

	// work out how much to back off by
	backoffDuration := time.Second * time.Duration(math.Pow(2, float64(s.backoff)))
	s.backoff++
	duration := backoffDuration

	// sleep for that amount of time
	logger.WithField("appservice", s.ID).WithField("backing off duration", duration.String()).WithError(err).Error("Unable to send transaction to appservice")
	time.Sleep(duration)

	return err
}

// appserviceIsInterestedInEvent returns a boolean depending on whether a given
// event falls within one of a given application service's namespaces.
//
// TODO: This should be cached, see https://github.com/antinvestor/matrix/issues/1682
func (s *OutputRoomEventConsumer) appserviceIsInterestedInEvent(ctx context.Context, event *types.HeaderedEvent, appservice *config.ApplicationService) bool {
	logger := util.Log(ctx)
	user := ""
	userID, err := s.rsAPI.QueryUserIDForSender(ctx, event.RoomID(), event.SenderID())
	if err == nil {
		user = userID.String()
	}

	switch {
	case appservice.URL == "":
		return false
	case appservice.IsInterestedInEventType(event.Type()):
		return true
	case appservice.IsInterestedInUserID(user):
		return true
	case appservice.IsInterestedInRoomID(event.RoomID().String()):
		return true
	}

	if event.Type() == spec.MRoomMember && event.StateKey() != nil {
		if appservice.IsInterestedInUserID(*event.StateKey()) {
			return true
		}
	}

	// Check all known room aliases of the room the event came from
	queryReq := api.GetAliasesForRoomIDRequest{RoomID: event.RoomID().String()}
	var queryRes api.GetAliasesForRoomIDResponse
	if err := s.rsAPI.GetAliasesForRoomID(ctx, &queryReq, &queryRes); err == nil {
		for _, alias := range queryRes.Aliases {
			if appservice.IsInterestedInRoomAlias(alias) {
				return true
			}
		}
	} else {
		logger.WithField("appservice", appservice.ID).WithField("room_id", event.RoomID().String()).WithError(err).Error("Unable to get aliases for room")
	}

	// Check if any of the members in the room match the appservice
	return s.appserviceJoinedAtEvent(ctx, event, appservice)
}

// appserviceJoinedAtEvent returns a boolean depending on whether a given
// appservice has membership at the time a given event was created.
func (s *OutputRoomEventConsumer) appserviceJoinedAtEvent(ctx context.Context, event *types.HeaderedEvent, appservice *config.ApplicationService) bool {
	logger := util.Log(ctx)
	// TODO: This is only checking the current room state, not the state at
	// the event in question. Pretty sure this is what Synapse does too, but
	// until we have a lighter way of checking the state before the event that
	// doesn't involve state res, then this is probably OK.
	membershipReq := &api.QueryMembershipsForRoomRequest{
		RoomID:     event.RoomID().String(),
		JoinedOnly: true,
	}
	membershipRes := &api.QueryMembershipsForRoomResponse{}

	// XXX: This could potentially race if the state for the event is not known yet
	// e.g. the event came over federation but we do not have the full state persisted.
	if err := s.rsAPI.QueryMembershipsForRoom(ctx, membershipReq, membershipRes); err == nil {
		for _, ev := range membershipRes.JoinEvents {
			switch {
			case ev.StateKey == nil:
				continue
			case ev.Type != spec.MRoomMember:
				continue
			}
			var membership gomatrixserverlib.MemberContent
			err = json.Unmarshal(ev.Content, &membership)
			switch {
			case err != nil:
				continue
			case membership.Membership == spec.Join:
				if appservice.IsInterestedInUserID(*ev.StateKey) {
					return true
				}
			}
		}
	} else {
		logger.WithField("appservice", appservice.ID).WithField("room_id", event.RoomID().String()).WithError(err).Error("Unable to get membership for room")
	}
	return false
}
