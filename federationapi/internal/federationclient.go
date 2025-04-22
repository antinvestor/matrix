package internal

import (
	"context"
	"time"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
)

const defaultTimeout = time.Second * 30

// Functions here are "proxying" calls to the gomatrixserverlib federation
// client.

func (r *FederationInternalAPI) MakeJoin(
	ctx context.Context, origin, s spec.ServerName, roomID, userID string,
) (res gomatrixserverlib.MakeJoinResponse, err error) {
	iCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	ires, err := r.federation.MakeJoin(iCtx, origin, s, roomID, userID)
	if err != nil {
		return &fclient.RespMakeJoin{}, err
	}
	return &ires, nil
}

func (r *FederationInternalAPI) SendJoin(
	ctx context.Context, origin, s spec.ServerName, event gomatrixserverlib.PDU,
) (res gomatrixserverlib.SendJoinResponse, err error) {
	iCtx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	ires, err := r.federation.SendJoin(iCtx, origin, s, event)
	if err != nil {
		return &fclient.RespSendJoin{}, err
	}
	return &ires, nil
}

func (r *FederationInternalAPI) GetEventAuth(
	ctx context.Context, origin, s spec.ServerName,
	roomVersion gomatrixserverlib.RoomVersion, roomID, eventID string,
) (res fclient.RespEventAuth, err error) {
	iCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	ires, err := r.doRequestIfNotBlacklisted(iCtx, s, func() (interface{}, error) {
		return r.federation.GetEventAuth(iCtx, origin, s, roomVersion, roomID, eventID)
	})
	if err != nil {
		return fclient.RespEventAuth{}, err
	}
	return ires.(fclient.RespEventAuth), nil
}

func (r *FederationInternalAPI) GetUserDevices(
	ctx context.Context, origin, s spec.ServerName, userID string,
) (fclient.RespUserDevices, error) {
	iCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	ires, err := r.doRequestIfNotBlacklisted(iCtx, s, func() (interface{}, error) {
		return r.federation.GetUserDevices(iCtx, origin, s, userID)
	})
	if err != nil {
		return fclient.RespUserDevices{}, err
	}
	return ires.(fclient.RespUserDevices), nil
}

func (r *FederationInternalAPI) ClaimKeys(
	ctx context.Context, origin, s spec.ServerName, oneTimeKeys map[string]map[string]string,
) (fclient.RespClaimKeys, error) {
	iCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	ires, err := r.doRequestIfNotBlacklisted(iCtx, s, func() (interface{}, error) {
		return r.federation.ClaimKeys(iCtx, origin, s, oneTimeKeys)
	})
	if err != nil {
		return fclient.RespClaimKeys{}, err
	}
	return ires.(fclient.RespClaimKeys), nil
}

func (r *FederationInternalAPI) QueryKeys(
	ctx context.Context, origin, s spec.ServerName, keys map[string][]string,
) (fclient.RespQueryKeys, error) {
	ires, err := r.doRequestIfNotBackingOffOrBlacklisted(ctx, s, func() (interface{}, error) {
		return r.federation.QueryKeys(ctx, origin, s, keys)
	})
	if err != nil {
		return fclient.RespQueryKeys{}, err
	}
	return ires.(fclient.RespQueryKeys), nil
}

func (r *FederationInternalAPI) Backfill(
	ctx context.Context, origin, s spec.ServerName, roomID string, limit int, eventIDs []string,
) (res gomatrixserverlib.Transaction, err error) {
	iCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	ires, err := r.doRequestIfNotBlacklisted(iCtx, s, func() (interface{}, error) {
		return r.federation.Backfill(iCtx, origin, s, roomID, limit, eventIDs)
	})
	if err != nil {
		return gomatrixserverlib.Transaction{}, err
	}
	return ires.(gomatrixserverlib.Transaction), nil
}

func (r *FederationInternalAPI) LookupState(
	ctx context.Context, origin, s spec.ServerName, roomID, eventID string, roomVersion gomatrixserverlib.RoomVersion,
) (res gomatrixserverlib.StateResponse, err error) {
	iCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	ires, err := r.doRequestIfNotBlacklisted(iCtx, s, func() (interface{}, error) {
		return r.federation.LookupState(iCtx, origin, s, roomID, eventID, roomVersion)
	})
	if err != nil {
		return &fclient.RespState{}, err
	}
	rs := ires.(fclient.RespState)
	return &rs, nil
}

func (r *FederationInternalAPI) LookupStateIDs(
	ctx context.Context, origin, s spec.ServerName, roomID, eventID string,
) (res gomatrixserverlib.StateIDResponse, err error) {
	iCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	ires, err := r.doRequestIfNotBlacklisted(iCtx, s, func() (interface{}, error) {
		return r.federation.LookupStateIDs(iCtx, origin, s, roomID, eventID)
	})
	if err != nil {
		return fclient.RespStateIDs{}, err
	}
	return ires.(fclient.RespStateIDs), nil
}

func (r *FederationInternalAPI) LookupMissingEvents(
	ctx context.Context, origin, s spec.ServerName, roomID string,
	missing fclient.MissingEvents, roomVersion gomatrixserverlib.RoomVersion,
) (res fclient.RespMissingEvents, err error) {
	iCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	ires, err := r.doRequestIfNotBlacklisted(iCtx, s, func() (interface{}, error) {
		return r.federation.LookupMissingEvents(iCtx, origin, s, roomID, missing, roomVersion)
	})
	if err != nil {
		return fclient.RespMissingEvents{}, err
	}
	return ires.(fclient.RespMissingEvents), nil
}

func (r *FederationInternalAPI) GetEvent(
	ctx context.Context, origin, s spec.ServerName, eventID string,
) (res gomatrixserverlib.Transaction, err error) {
	iCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	ires, err := r.doRequestIfNotBlacklisted(iCtx, s, func() (interface{}, error) {
		return r.federation.GetEvent(iCtx, origin, s, eventID)
	})
	if err != nil {
		return gomatrixserverlib.Transaction{}, err
	}
	return ires.(gomatrixserverlib.Transaction), nil
}

func (r *FederationInternalAPI) LookupServerKeys(
	ctx context.Context, s spec.ServerName, keyRequests map[gomatrixserverlib.PublicKeyLookupRequest]spec.Timestamp,
) ([]gomatrixserverlib.ServerKeys, error) {
	iCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	ires, err := r.doRequestIfNotBlacklisted(iCtx, s, func() (interface{}, error) {
		return r.federation.LookupServerKeys(iCtx, s, keyRequests)
	})
	if err != nil {
		return []gomatrixserverlib.ServerKeys{}, err
	}
	return ires.([]gomatrixserverlib.ServerKeys), nil
}

func (r *FederationInternalAPI) MSC2836EventRelationships(
	ctx context.Context, origin, s spec.ServerName, rr fclient.MSC2836EventRelationshipsRequest,
	roomVersion gomatrixserverlib.RoomVersion,
) (res fclient.MSC2836EventRelationshipsResponse, err error) {
	iCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	ires, err := r.doRequestIfNotBlacklisted(iCtx, s, func() (interface{}, error) {
		return r.federation.MSC2836EventRelationships(iCtx, origin, s, rr, roomVersion)
	})
	if err != nil {
		return res, err
	}
	return ires.(fclient.MSC2836EventRelationshipsResponse), nil
}

func (r *FederationInternalAPI) RoomHierarchies(
	ctx context.Context, origin, s spec.ServerName, roomID string, suggestedOnly bool,
) (res fclient.RoomHierarchyResponse, err error) {
	iCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	ires, err := r.doRequestIfNotBlacklisted(iCtx, s, func() (interface{}, error) {
		return r.federation.RoomHierarchy(iCtx, origin, s, roomID, suggestedOnly)
	})
	if err != nil {
		return res, err
	}
	return ires.(fclient.RoomHierarchyResponse), nil
}
