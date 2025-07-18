package streams

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math"
	"strconv"
	"time"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/syncapi/synctypes"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/pitabwire/util"
)

type InviteStreamProvider struct {
	DefaultStreamProvider
	rsAPI api.SyncRoomserverAPI
}

func (p *InviteStreamProvider) Setup(
	ctx context.Context,
) {
	p.DefaultStreamProvider.Setup(ctx)

	p.latestMutex.Lock()
	defer p.latestMutex.Unlock()

	id, err := p.DB.MaxStreamPositionForInvites(ctx)
	if err != nil {
		panic(err)
	}
	p.latest = id
}

func (p *InviteStreamProvider) CompleteSync(
	ctx context.Context,
	req *types.SyncRequest,
) types.StreamPosition {
	return p.IncrementalSync(ctx, req, 0, p.LatestPosition(ctx))
}

func (p *InviteStreamProvider) IncrementalSync(
	ctx context.Context,
	req *types.SyncRequest,
	from, to types.StreamPosition,
) types.StreamPosition {

	log := util.Log(ctx)
	r := types.Range{
		From: from,
		To:   to,
	}

	invites, retiredInvites, maxID, err := p.DB.InviteEventsInRange(
		ctx, req.Device.UserID, r,
	)
	if err != nil {
		log.WithError(err).Error("p.Cm.InviteEventsInRange failed")
		return from
	}

	eventFormat := synctypes.FormatSync
	if req.Filter.EventFormat == synctypes.EventFormatFederation {
		eventFormat = synctypes.FormatSyncFederation
	}

	for roomID, inviteEvent := range invites {
		user := spec.UserID{}
		sender, err := p.rsAPI.QueryUserIDForSender(ctx, inviteEvent.RoomID(), inviteEvent.SenderID())
		if err == nil && sender != nil {
			user = *sender
		}

		// skip ignored user events
		if _, ok := req.IgnoredUsers.List[user.String()]; ok {
			continue
		}
		ir, err := types.NewInviteResponse(ctx, p.rsAPI, inviteEvent, eventFormat)
		if err != nil {
			log.WithError(err).Error("failed creating invite response")
			continue
		}
		req.Response.Rooms.Invite[roomID] = ir
	}

	// When doing an initial sync, we don't want to add retired invites, as this
	// can add rooms we were invited to, but already left.
	if from == 0 {
		return to
	}
	for roomID := range retiredInvites {
		membership, _, err := p.DB.SelectMembershipForUser(ctx, roomID, req.Device.UserID, math.MaxInt64)
		// Skip if the user is an existing member of the room.
		// Otherwise, the NewLeaveResponse will eject the user from the room unintentionally
		if membership == spec.Join ||
			err != nil {
			continue
		}

		lr := types.NewLeaveResponse()
		h := sha256.Sum256(append([]byte(roomID), []byte(strconv.FormatInt(int64(to), 10))...))
		lr.Timeline.Events = append(lr.Timeline.Events, synctypes.ClientEvent{
			// fake event ID which muxes in the to position
			EventID:        "$" + base64.RawURLEncoding.EncodeToString(h[:]),
			OriginServerTS: spec.AsTimestamp(time.Now()),
			RoomID:         roomID,
			Sender:         req.Device.UserID,
			StateKey:       &req.Device.UserID,
			Type:           "m.room.member",
			Content:        json.RawMessage(`{"membership":"leave"}`),
		})
		req.Response.Rooms.Leave[roomID] = lr
	}

	return maxID
}
