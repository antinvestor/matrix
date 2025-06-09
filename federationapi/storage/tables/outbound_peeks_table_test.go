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

func mustCreateOutboundpeeksTable(ctx context.Context, svc *frame.Service, t *testing.T, _ test.DependancyOption) tables.FederationOutboundPeeks {

	cm := sqlutil.NewConnectionManager(svc)
	var tab tables.FederationOutboundPeeks
	tab, err := postgres.NewPostgresOutboundPeeksTable(ctx, cm)

	if err != nil {
		t.Fatal("failed to create table: %s", err)
	}
	err = cm.Migrate(ctx)
	if err != nil {
		t.Fatal("failed to migrate table: %s", err)
	}
	return tab
}

func TestOutboundPeeksTable(t *testing.T) {

	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	_, serverName, _ := gomatrixserverlib.SplitID('@', alice.ID)
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		tab := mustCreateOutboundpeeksTable(ctx, svc, t, testOpts)

		// Insert a peek
		peekID := util.RandomString(8)
		var renewalInterval int64 = 1000
		if err := tab.InsertOutboundPeek(ctx, serverName, room.ID, peekID, renewalInterval); err != nil {
			t.Fatal(err)
		}

		// select the newly inserted peek
		outboundPeek1, err := tab.SelectOutboundPeek(ctx, serverName, room.ID, peekID)
		if err != nil {
			t.Fatal(err)
		}

		// Assert fields are set as expected
		if outboundPeek1.PeekID != peekID {
			t.Fatal("unexpected outbound peek ID: %s, want %s", outboundPeek1.PeekID, peekID)
		}
		if outboundPeek1.RoomID != room.ID {
			t.Fatal("unexpected outbound peek room ID: %s, want %s", outboundPeek1.RoomID, peekID)
		}
		if outboundPeek1.ServerName != serverName {
			t.Fatal("unexpected outbound peek servername: %s, want %s", outboundPeek1.ServerName, serverName)
		}
		if outboundPeek1.RenewalInterval != renewalInterval {
			t.Fatal("unexpected outbound peek renewal interval: %d, want %d", outboundPeek1.RenewalInterval, renewalInterval)
		}

		// Renew the peek
		if err = tab.RenewOutboundPeek(ctx, serverName, room.ID, peekID, 2000); err != nil {
			t.Fatal(err)
		}

		// verify the values changed
		outboundPeek2, err := tab.SelectOutboundPeek(ctx, serverName, room.ID, peekID)
		if err != nil {
			t.Fatal(err)
		}
		if reflect.DeepEqual(outboundPeek1, outboundPeek2) {
			t.Fatal("expected a change peek, but they are the same")
		}
		if outboundPeek1.ServerName != outboundPeek2.ServerName {
			t.Fatal("unexpected servername change: %s -> %s", outboundPeek1.ServerName, outboundPeek2.ServerName)
		}
		if outboundPeek1.RoomID != outboundPeek2.RoomID {
			t.Fatal("unexpected roomID change: %s -> %s", outboundPeek1.RoomID, outboundPeek2.RoomID)
		}

		// delete the peek
		if err = tab.DeleteOutboundPeek(ctx, serverName, room.ID, peekID); err != nil {
			t.Fatal(err)
		}

		// There should be no peek anymore
		peek, err := tab.SelectOutboundPeek(ctx, serverName, room.ID, peekID)
		if err != nil {
			t.Fatal(err)
		}
		if peek != nil {
			t.Fatal("got a peek which should be deleted: %+v", peek)
		}

		// insert some peeks
		var peekIDs []string
		for i := 0; i < 5; i++ {
			peekID = util.RandomString(8)
			if err = tab.InsertOutboundPeek(ctx, serverName, room.ID, peekID, 1000); err != nil {
				t.Fatal(err)
			}
			peekIDs = append(peekIDs, peekID)
		}

		// Now select them
		outboundPeeks, err := tab.SelectOutboundPeeks(ctx, room.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(outboundPeeks) != len(peekIDs) {
			t.Fatal("inserted %d peeks, selected %d", len(peekIDs), len(outboundPeeks))
		}
		gotPeekIDs := make([]string, 0, len(outboundPeeks))
		for _, p := range outboundPeeks {
			gotPeekIDs = append(gotPeekIDs, p.PeekID)
		}
		assert.ElementsMatch(t, gotPeekIDs, peekIDs)

		// And delete them again
		if err = tab.DeleteOutboundPeeks(ctx, room.ID); err != nil {
			t.Fatal(err)
		}

		// they should be gone now
		outboundPeeks, err = tab.SelectOutboundPeeks(ctx, room.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(outboundPeeks) > 0 {
			t.Fatal("got outbound peeks which should be deleted")
		}
	})
}
