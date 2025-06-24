package shared

import (
	"context"
	"fmt"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
)

type MembershipUpdater struct {
	d             *Database
	roomNID       types.RoomNID
	targetUserNID types.EventStateKeyNID
	oldMembership tables.MembershipState
}

func NewMembershipUpdater(
	ctx context.Context, d *Database, roomID, targetUserID string,
	targetLocal bool, roomVersion gomatrixserverlib.RoomVersion,
) (context.Context, *MembershipUpdater, error) {
	var roomNID types.RoomNID
	var targetUserNID types.EventStateKeyNID
	var err error
	err = d.Cm.Do(ctx, func(ctx context.Context) error {
		roomNID, err = d.assignRoomNID(ctx, roomID, roomVersion)
		if err != nil {
			return err
		}
		targetUserNID, err = d.assignStateKeyNID(ctx, targetUserID)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return ctx, nil, err
	}

	return d.membershipUpdaterTxn(ctx, roomNID, targetUserNID, targetLocal)
}

func (d *Database) membershipUpdaterTxn(
	ctx context.Context,
	roomNID types.RoomNID,
	targetUserNID types.EventStateKeyNID,
	targetLocal bool,
) (context.Context, *MembershipUpdater, error) {
	err := d.MembershipTable.InsertMembership(ctx, roomNID, targetUserNID, targetLocal)
	if err != nil {
		return ctx, nil, fmt.Errorf("d.MembershipTable.InsertMembership: %w", err)
	}

	membership, err := d.MembershipTable.SelectMembershipForUpdate(ctx, roomNID, targetUserNID)
	if err != nil {
		return ctx, nil, err
	}

	return ctx, &MembershipUpdater{
		d, roomNID, targetUserNID, membership,
	}, nil
}

// IsInvite implements types.MembershipUpdater
func (u *MembershipUpdater) IsInvite() bool {
	return u.oldMembership == tables.MembershipStateInvite
}

// IsJoin implements types.MembershipUpdater
func (u *MembershipUpdater) IsJoin() bool {
	return u.oldMembership == tables.MembershipStateJoin
}

// IsLeave implements types.MembershipUpdater
func (u *MembershipUpdater) IsLeave() bool {
	return u.oldMembership == tables.MembershipStateLeaveOrBan
}

// IsKnock implements types.MembershipUpdater
func (u *MembershipUpdater) IsKnock() bool {
	return u.oldMembership == tables.MembershipStateKnock
}

func (u *MembershipUpdater) Delete(ctx context.Context) error {
	if _, err := u.d.InvitesTable.UpdateInviteRetired(ctx, u.roomNID, u.targetUserNID); err != nil {
		return err
	}
	return u.d.MembershipTable.DeleteMembership(ctx, u.roomNID, u.targetUserNID)
}

func (u *MembershipUpdater) Update(ctx context.Context, newMembership tables.MembershipState, event *types.Event) (bool, []string, error) {
	var inserted bool    // Did the query result in a membership change?
	var retired []string // Did we retire any updates in the process?
	return inserted, retired, u.d.Cm.Do(ctx, func(ctx context.Context) error {
		senderUserNID, err := u.d.assignStateKeyNID(ctx, string(event.SenderID()))
		if err != nil {
			return fmt.Errorf("u.d.AssignStateKeyNID: %w", err)
		}
		inserted, err = u.d.MembershipTable.UpdateMembership(ctx, u.roomNID, u.targetUserNID, senderUserNID, newMembership, event.EventNID, false)
		if err != nil {
			return fmt.Errorf("u.d.MembershipTable.UpdateMembership: %w", err)
		}
		if !inserted {
			return nil
		}
		switch {
		case u.oldMembership != tables.MembershipStateInvite && newMembership == tables.MembershipStateInvite:
			inserted, err = u.d.InvitesTable.InsertInviteEvent(
				ctx, event.EventID(), u.roomNID, u.targetUserNID, senderUserNID, event.JSON(),
			)
			if err != nil {
				return fmt.Errorf("u.d.InvitesTable.InsertInviteEvent: %w", err)
			}
		case u.oldMembership == tables.MembershipStateInvite && newMembership != tables.MembershipStateInvite:
			retired, err = u.d.InvitesTable.UpdateInviteRetired(
				ctx, u.roomNID, u.targetUserNID,
			)
			if err != nil {
				return fmt.Errorf("u.d.InvitesTables.UpdateInviteRetired: %w", err)
			}
		}
		return nil
	})
}
