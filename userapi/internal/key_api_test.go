package internal_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/internal"
	"github.com/antinvestor/matrix/userapi/storage"
)

func mustCreateDatabase(ctx context.Context, t *testing.T, _ test.DependancyOption) (storage.KeyDatabase, func()) {
	t.Helper()

	connStr, closeDb, err := test.PrepareDatabaseDSConnection(ctx)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	cm := sqlutil.NewConnectionManager(ctx, config.DatabaseOptions{ConnectionString: connStr})
	db, err := storage.NewKeyDatabase(ctx, cm, &config.DatabaseOptions{
		ConnectionString:   connStr,
		MaxOpenConnections: 10,
	})
	if err != nil {
		t.Fatalf("failed to create new user db: %v", err)
	}
	return db, closeDb
}

func Test_QueryDeviceMessages(t *testing.T) {
	alice := test.NewUser(t)
	type args struct {
		req *api.QueryDeviceMessagesRequest
		res *api.QueryDeviceMessagesResponse
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		want    *api.QueryDeviceMessagesResponse
	}{
		{
			name: "no existing keys",
			args: args{
				req: &api.QueryDeviceMessagesRequest{
					UserID: "@doesNotExist:localhost",
				},
				res: &api.QueryDeviceMessagesResponse{},
			},
			want: &api.QueryDeviceMessagesResponse{},
		},
		{
			name: "existing user returns devices",
			args: args{
				req: &api.QueryDeviceMessagesRequest{
					UserID: alice.ID,
				},
				res: &api.QueryDeviceMessagesResponse{},
			},
			want: &api.QueryDeviceMessagesResponse{
				StreamID: 6,
				Devices: []api.DeviceMessage{
					{
						Type: api.TypeDeviceKeyUpdate, StreamID: 5, DeviceKeys: &api.DeviceKeys{
							DeviceID:    "myDevice",
							DisplayName: "first device",
							UserID:      alice.ID,
							KeyJSON:     []byte("ghi"),
						},
					},
					{
						Type: api.TypeDeviceKeyUpdate, StreamID: 6, DeviceKeys: &api.DeviceKeys{
							DeviceID:    "mySecondDevice",
							DisplayName: "second device",
							UserID:      alice.ID,
							KeyJSON:     []byte("jkl"),
						}, // streamID 6
					},
				},
			},
		},
	}

	deviceMessages := []api.DeviceMessage{
		{ // not the user we're looking for
			Type: api.TypeDeviceKeyUpdate, DeviceKeys: &api.DeviceKeys{
				UserID: "@doesNotExist:localhost",
			},
			// streamID 1 for this user
		},
		{ // empty keyJSON will be ignored
			Type: api.TypeDeviceKeyUpdate, DeviceKeys: &api.DeviceKeys{
				DeviceID: "myDevice",
				UserID:   alice.ID,
			}, // streamID 1
		},
		{
			Type: api.TypeDeviceKeyUpdate, DeviceKeys: &api.DeviceKeys{
				DeviceID: "myDevice",
				UserID:   alice.ID,
				KeyJSON:  []byte("abc"),
			}, // streamID 2
		},
		{
			Type: api.TypeDeviceKeyUpdate, DeviceKeys: &api.DeviceKeys{
				DeviceID: "myDevice",
				UserID:   alice.ID,
				KeyJSON:  []byte("def"),
			}, // streamID 3
		},
		{
			Type: api.TypeDeviceKeyUpdate, DeviceKeys: &api.DeviceKeys{
				DeviceID: "myDevice",
				UserID:   alice.ID,
				KeyJSON:  []byte(""),
			}, // streamID 4
		},
		{
			Type: api.TypeDeviceKeyUpdate, DeviceKeys: &api.DeviceKeys{
				DeviceID:    "myDevice",
				DisplayName: "first device",
				UserID:      alice.ID,
				KeyJSON:     []byte("ghi"),
			}, // streamID 5
		},
		{
			Type: api.TypeDeviceKeyUpdate, DeviceKeys: &api.DeviceKeys{
				DeviceID:    "mySecondDevice",
				UserID:      alice.ID,
				KeyJSON:     []byte("jkl"),
				DisplayName: "second device",
			}, // streamID 6
		},
	}

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx := testrig.NewContext(t)
		db, closeDB := mustCreateDatabase(ctx, t, testOpts)
		defer closeDB()
		if err := db.StoreLocalDeviceKeys(ctx, deviceMessages); err != nil {
			t.Fatalf("failed to store local devicesKeys")
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				a := &internal.UserInternalAPI{
					KeyDatabase: db,
				}
				if err := a.QueryDeviceMessages(ctx, tt.args.req, tt.args.res); (err != nil) != tt.wantErr {
					t.Errorf("QueryDeviceMessages() error = %v, wantErr %v", err, tt.wantErr)
				}
				got := tt.args.res
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("QueryDeviceMessages(): got:\n%+v, want:\n%+v", got, tt.want)
				}
			})
		}
	})
}
