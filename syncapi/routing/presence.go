package routing

import (
	"context"

	"connectrpc.com/connect"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/syncapi/storage"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/gorilla/mux"

	"buf.build/gen/go/antinvestor/presence/connectrpc/go/presencev1connect"
	presenceV1 "buf.build/gen/go/antinvestor/presence/protocolbuffers/go"
)

// PresenceServer implements the PresenceService connect interface
type PresenceServer struct {
	presencev1connect.UnimplementedPresenceServiceHandler

	db     storage.Database
	devAPI api.SyncUserAPI
}

// SetupPresenceServer creates a new PresenceServer
func SetupPresenceServer(mux *mux.Router, validator connect.Interceptor, db storage.Database, devAPI api.SyncUserAPI) error {

	path, handler := presencev1connect.NewPresenceServiceHandler(&PresenceServer{
		db:     db,
		devAPI: devAPI,
	}, connect.WithInterceptors(validator))
	mux.Handle(path, handler)

	return nil
}

// GetPresence implements the gRPC GetPresence method
func (s *PresenceServer) GetPresence(ctx context.Context, req *connect.Request[presenceV1.GetPresenceRequest]) (*connect.Response[presenceV1.GetPresenceResponse], error) {

	userID := req.Msg.GetUserId()

	// Get presence information for the user
	presences, err := s.db.GetPresences(ctx, []string{userID})
	if err != nil {
		return nil, err
	}

	presence := &types.PresenceInternal{
		UserID: userID,
	}
	if len(presences) > 0 {
		presence = presences[0]
	}

	deviceRes := api.QueryDevicesResponse{}
	if err = s.devAPI.QueryDevices(ctx, &api.QueryDevicesRequest{UserID: userID}, &deviceRes); err != nil {
		return nil, err
	}

	for i := range deviceRes.Devices {
		if int64(presence.LastActiveTS) < deviceRes.Devices[i].LastSeenTS {
			presence.LastActiveTS = spec.Timestamp(deviceRes.Devices[i].LastSeenTS)
		}
	}

	// Convert database presence to response
	p := types.PresenceInternal{LastActiveTS: spec.Timestamp(presence.LastActiveTS)}
	currentlyActive := p.CurrentlyActive()

	return connect.NewResponse(&presenceV1.GetPresenceResponse{
		Presence:        presence.ClientFields.Presence,
		StatusMsg:       presence.ClientFields.StatusMsg,
		LastActiveTs:    int64(presence.LastActiveTS),
		CurrentlyActive: currentlyActive,
		LastActiveAgo:   p.LastActiveAgo(),
	}), nil
}
