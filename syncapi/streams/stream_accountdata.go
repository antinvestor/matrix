package streams

import (
	"context"
	"encoding/json"

	"github.com/antinvestor/matrix/syncapi/storage"
	"github.com/antinvestor/matrix/syncapi/synctypes"
	"github.com/antinvestor/matrix/syncapi/types"
	userapi "github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/util"
)

type AccountDataStreamProvider struct {
	DefaultStreamProvider
	userAPI userapi.SyncUserAPI
}

func (p *AccountDataStreamProvider) Setup(
	ctx context.Context, snapshot storage.DatabaseTransaction,
) {
	p.DefaultStreamProvider.Setup(ctx, snapshot)

	p.latestMutex.Lock()
	defer p.latestMutex.Unlock()

	id, err := snapshot.MaxStreamPositionForAccountData(ctx)
	if err != nil {
		panic(err)
	}
	p.latest = id
}

func (p *AccountDataStreamProvider) CompleteSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
) types.StreamPosition {
	return p.IncrementalSync(ctx, snapshot, req, 0, p.LatestPosition(ctx))
}

func (p *AccountDataStreamProvider) IncrementalSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
	from, to types.StreamPosition,
) types.StreamPosition {

	log := util.Log(ctx)

	r := types.Range{
		From: from,
		To:   to,
	}

	dataTypes, pos, err := snapshot.GetAccountDataInRange(
		ctx, req.Device.UserID, r, &req.Filter.AccountData,
	)
	if err != nil {
		log.WithError(err).Error("p.Cm.GetAccountDataInRange failed")
		return from
	}

	// Iterate over the rooms
	for roomID, roomDataTypes := range dataTypes {
		// For a complete sync, make sure we're only including this room if
		// that room was present in the joined rooms.
		if from == 0 && roomID != "" && !req.IsRoomPresent(roomID) {
			continue
		}

		// Request the missing data from the database
		for _, dataType := range roomDataTypes {
			dataReq := userapi.QueryAccountDataRequest{
				UserID:   req.Device.UserID,
				RoomID:   roomID,
				DataType: dataType,
			}
			dataRes := userapi.QueryAccountDataResponse{}
			err = p.userAPI.QueryAccountData(ctx, &dataReq, &dataRes)
			if err != nil {
				log.WithError(err).Error("p.userAPI.QueryAccountData failed")
				continue
			}
			if roomID == "" {
				if globalData, ok := dataRes.GlobalAccountData[dataType]; ok {
					req.Response.AccountData.Events = append(
						req.Response.AccountData.Events,
						synctypes.ClientEvent{
							Type:    dataType,
							Content: json.RawMessage(globalData),
						},
					)
				}
			} else {
				if roomData, ok := dataRes.RoomAccountData[roomID][dataType]; ok {
					joinData, joinOK := req.Response.Rooms.Join[roomID]
					if !joinOK {
						joinData = types.NewJoinResponse()
					}
					joinData.AccountData.Events = append(
						joinData.AccountData.Events,
						synctypes.ClientEvent{
							Type:    dataType,
							Content: json.RawMessage(roomData),
						},
					)
					req.Response.Rooms.Join[roomID] = joinData
				}
			}
		}
	}

	return pos
}
