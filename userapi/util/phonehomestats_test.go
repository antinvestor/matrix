package util

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/test/testrig"
	"github.com/antinvestor/matrix/userapi/storage"
	"golang.org/x/crypto/bcrypt"
)

func TestCollect(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		cm := sqlutil.NewConnectionManager(svc)
		db, err := storage.NewUserDatabase(ctx, nil, nil, cm, "localhost", bcrypt.MinCost, 1000, 1000, "")
		if err != nil {
			t.Error(err)
		}

		receivedRequest := make(chan struct{}, 1)
		// create a test server which responds to our call
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var data map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
				t.Error(err)
			}
			defer r.Body.Close()
			if _, err := w.Write([]byte("{}")); err != nil {
				t.Error(err)
			}

			// verify the received data matches our expectations
			dbEngine, ok := data["database_engine"]
			if !ok {
				t.Errorf("missing database_engine in JSON request: %+v", data)
			}
			version, ok := data["version"]
			if !ok {
				t.Errorf("missing version in JSON request: %+v", data)
			}
			if version != internal.VersionString() {
				t.Errorf("unexpected version: %q, expected %q", version, internal.VersionString())
			}
			switch {
			case testOpts == test.DependancyOption{} && dbEngine != "Postgres":
				t.Errorf("unexpected database_engine: %s", dbEngine)
			}
			close(receivedRequest)
		}))
		defer srv.Close()

		cfg.Global.ReportStats.Endpoint = srv.URL
		stats := phoneHomeStats{
			prevData:   timestampToRUUsage{},
			serverName: "localhost",
			startTime:  time.Now(),
			cfg:        cfg,
			db:         db,
			isMonolith: false,
			client:     &http.Client{Timeout: time.Second},
		}

		stats.collect(ctx)

		select {
		case <-time.After(time.Second * 5):
			t.Errorf("timed out waiting for response")
		case <-receivedRequest:
		}
	})
}
