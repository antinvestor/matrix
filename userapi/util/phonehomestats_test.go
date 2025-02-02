package util

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"golang.org/x/crypto/bcrypt"

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/test/testrig"
	"github.com/antinvestor/matrix/userapi/storage"
)

func TestCollect(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, testOpts)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		db, err := storage.NewUserDatabase(processCtx.Context(), nil, cm, &cfg.UserAPI.AccountDatabase, "localhost", bcrypt.MinCost, 1000, 1000, "")
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

		stats.collect()

		select {
		case <-time.After(time.Second * 5):
			t.Error("timed out waiting for response")
		case <-receivedRequest:
		}
	})
}
