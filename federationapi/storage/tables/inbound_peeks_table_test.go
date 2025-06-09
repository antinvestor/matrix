package tables_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/pitabwire/frame"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/matrix/federationapi/storage/postgres"
	"github.com/antinvestor/matrix/federationapi/storage/tables"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/test"
	"github.com/pitabwire/util"
	"github.com/stretchr/testify/assert"
)

func mustCreateInboundpeeksTable(ctx context.Context, svc *frame.Service, t *testing.T, _ test.DependancyOption) tables.FederationInboundPeeks {

	cm := sqlutil.NewConnectionManager(svc)
	var tab tables.FederationInboundPeeks
	tab, err := postgres.NewPostgresInboundPeeksTable(ctx, cm)

	if err != nil {
		t.Fatalf("failed to create table: %s", err)
	}
	err = cm.Migrate(ctx)
	if err != nil {
		t.Fatalf("failed to migrate table: %s", err)
	}
	return tab
}

func TestInboundPeeksTable(t *testing.T) {

	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	_, serverName, _ := gomatrixserverlib.SplitID('@', alice.ID)
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		tab := mustCreateInboundpeeksTable(ctx, svc, t, testOpts)

		// Insert a peek
		peekID := util.RandomString(8)
		var renewalInterval int64 = 1000
		if err := tab.InsertInboundPeek(ctx, serverName, room.ID, peekID, renewalInterval); err != nil {
			t.Fatal(err)
		}

		// select the newly inserted peek
		inboundPeek1, err := tab.SelectInboundPeek(ctx, serverName, room.ID, peekID)
		if err != nil {
			t.Fatal(err)
		}

		// Assert fields are set as expected
		if inboundPeek1.PeekID != peekID {
			t.Fatalf("unexpected inbound peek ID: %s, want %s", inboundPeek1.PeekID, peekID)
		}
		if inboundPeek1.RoomID != room.ID {
			t.Fatalf("unexpected inbound peek room ID: %s, want %s", inboundPeek1.RoomID, peekID)
		}
		if inboundPeek1.ServerName != serverName {
			t.Fatalf("unexpected inbound peek servername: %s, want %s", inboundPeek1.ServerName, serverName)
		}
		if inboundPeek1.RenewalInterval != renewalInterval {
			t.Fatalf("unexpected inbound peek renewal interval: %d, want %d", inboundPeek1.RenewalInterval, renewalInterval)
		}

		// Renew the peek
		if err = tab.RenewInboundPeek(ctx, serverName, room.ID, peekID, 2000); err != nil {
			t.Fatal(err)
		}

		// verify the values changed
		inboundPeek2, err := tab.SelectInboundPeek(ctx, serverName, room.ID, peekID)
		if err != nil {
			t.Fatal(err)
		}
		if reflect.DeepEqual(inboundPeek1, inboundPeek2) {
			t.Fatalf("expected a change peek, but they are the same")
		}
		if inboundPeek1.ServerName != inboundPeek2.ServerName {
			t.Fatalf("unexpected servername change: %s -> %s", inboundPeek1.ServerName, inboundPeek2.ServerName)
		}
		if inboundPeek1.RoomID != inboundPeek2.RoomID {
			t.Fatalf("unexpected roomID change: %s -> %s", inboundPeek1.RoomID, inboundPeek2.RoomID)
		}

		// delete the peek
		if err = tab.DeleteInboundPeek(ctx, serverName, room.ID, peekID); err != nil {
			t.Fatal(err)
		}

		// There should be no peek anymore
		peek, err := tab.SelectInboundPeek(ctx, serverName, room.ID, peekID)
		if err != nil {
			t.Fatal(err)
		}
		if peek != nil {
			t.Fatalf("got a peek which should be deleted: %+v", peek)
		}

		// insert some peeks
		var peekIDs []string
		for i := 0; i < 5; i++ {
			peekID = util.RandomString(8)
			if err = tab.InsertInboundPeek(ctx, serverName, room.ID, peekID, 1000); err != nil {
				t.Fatal(err)
			}
			peekIDs = append(peekIDs, peekID)
		}

		// Now select them
		inboundPeeks, err := tab.SelectInboundPeeks(ctx, room.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(inboundPeeks) != len(peekIDs) {
			t.Fatalf("inserted %d peeks, selected %d", len(peekIDs), len(inboundPeeks))
		}
		gotPeekIDs := make([]string, 0, len(inboundPeeks))
		for _, p := range inboundPeeks {
			gotPeekIDs = append(gotPeekIDs, p.PeekID)
		}
		assert.ElementsMatch(t, gotPeekIDs, peekIDs)

		// And delete them again
		if err = tab.DeleteInboundPeeks(ctx, room.ID); err != nil {
			t.Fatal(err)
		}

		// they should be gone now
		inboundPeeks, err = tab.SelectInboundPeeks(ctx, room.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(inboundPeeks) > 0 {
			t.Fatalf("got inbound peeks which should be deleted")
		}

	})
}
