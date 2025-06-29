package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/test/testrig"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/util"
)

var (
	serverName = spec.ServerName("example.com")
	// space separated localpart+password -> account
	lookup = make(map[string]*api.Account)
	device = &api.Device{
		AccessToken: "flibble",
		DisplayName: "My Device",
		ID:          "device_id_goes_here",
	}
)

type fakeAccountDatabase struct{}

func (d *fakeAccountDatabase) PerformPasswordUpdate(ctx context.Context, req *api.PerformPasswordUpdateRequest, res *api.PerformPasswordUpdateResponse) error {
	return nil
}

func (d *fakeAccountDatabase) PerformAccountDeactivation(ctx context.Context, req *api.PerformAccountDeactivationRequest, res *api.PerformAccountDeactivationResponse) error {
	return nil
}

func (d *fakeAccountDatabase) QueryAccountByPassword(ctx context.Context, req *api.QueryAccountByPasswordRequest, res *api.QueryAccountByPasswordResponse) error {
	acc, ok := lookup[req.Localpart+" "+req.PlaintextPassword]
	if !ok {
		return fmt.Errorf("unknown user/password")
	}
	res.Account = acc
	res.Exists = true
	return nil
}

func setup() *UserInteractive {
	cfg := &config.ClientAPI{
		Global: &config.Global{
			SigningIdentity: fclient.SigningIdentity{
				ServerName: serverName,
			},
		},
	}
	return NewUserInteractive(&fakeAccountDatabase{}, cfg)
}

func TestUserInteractiveChallenge(t *testing.T) {

	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)
	uia := setup()
	// no auth key results in a challenge
	_, errRes := uia.Verify(ctx, []byte(`{}`), device)
	if errRes == nil {
		t.Fatalf("Verify succeeded with {} but expected failure")
		return
	}
	if errRes.Code != 401 {
		t.Errorf("Expected HTTP 401, got %d", errRes.Code)
	}
}

func TestUserInteractivePasswordLogin(t *testing.T) {

	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)
	uia := setup()
	// valid password login succeeds when an account exists
	lookup["alice herpassword"] = &api.Account{
		Localpart:  "alice",
		ServerName: serverName,
		UserID:     fmt.Sprintf("@alice:%s", serverName),
	}
	// valid password requests
	testCases := []json.RawMessage{
		// deprecated form
		[]byte(`{
			"auth": {
				"type": "m.login.password",
				"user": "alice",
				"password": "herpassword"
			}
		}`),
		// new form
		[]byte(`{
			"auth": {
				"type": "m.login.password",
				"identifier": {
					"type": "m.id.user",
					"user": "alice"
				},
				"password": "herpassword"
			}
		}`),
	}
	for _, tc := range testCases {
		_, errRes := uia.Verify(ctx, tc, device)
		if errRes != nil {
			t.Errorf("Verify failed but expected success for request: %s - got %+v", string(tc), errRes)
		}
	}
}

func TestUserInteractivePasswordBadLogin(t *testing.T) {

	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)
	uia := setup()
	// password login fails when an account exists but is specced wrong
	lookup["bob hispassword"] = &api.Account{
		Localpart:  "bob",
		ServerName: serverName,
		UserID:     fmt.Sprintf("@bob:%s", serverName),
	}
	// invalid password requests
	testCases := []struct {
		body    json.RawMessage
		wantRes util.JSONResponse
	}{
		{
			// fields not in an auth dict
			body: []byte(`{
				"type": "m.login.password",
				"user": "bob",
				"password": "hispassword"
			}`),
			wantRes: util.JSONResponse{
				Code: 401,
			},
		},
		{
			// wrong type
			body: []byte(`{
				"auth": {
					"type": "m.login.not_password",
					"identifier": {
						"type": "m.id.user",
						"user": "bob"
					},
					"password": "hispassword"
				}
			}`),
			wantRes: util.JSONResponse{
				Code: 400,
			},
		},
		{
			// identifier type is wrong
			body: []byte(`{
				"auth": {
					"type": "m.login.password",
					"identifier": {
						"type": "m.id.thirdparty",
						"user": "bob"
					},
					"password": "hispassword"
				}
			}`),
			wantRes: util.JSONResponse{
				Code: 401,
			},
		},
		{
			// wrong password
			body: []byte(`{
				"auth": {
					"type": "m.login.password",
					"identifier": {
						"type": "m.id.user",
						"user": "bob"
					},
					"password": "not_his_password"
				}
			}`),
			wantRes: util.JSONResponse{
				Code: 401,
			},
		},
	}
	for _, tc := range testCases {
		_, errRes := uia.Verify(ctx, tc.body, device)
		if errRes == nil {
			t.Errorf("Verify succeeded but expected failure for request: %s", string(tc.body))
			continue
		}
		if errRes.Code != tc.wantRes.Code {
			t.Errorf("got code %d want code %d for request: %s", errRes.Code, tc.wantRes.Code, string(tc.body))
		}
	}
}

func TestUserInteractive_AddCompletedStage(t *testing.T) {

	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)
	tests := []struct {
		name      string
		sessionID string
	}{
		{
			name:      "first user",
			sessionID: util.RandomString(8),
		},
		{
			name:      "second user",
			sessionID: util.RandomString(8),
		},
		{
			name:      "third user",
			sessionID: util.RandomString(8),
		},
	}
	u := setup()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, resp := u.Verify(ctx, []byte("{}"), nil)
			challenge, ok := resp.JSON.(Challenge)
			if !ok {
				t.Fatalf("expected a Challenge, got %T", resp.JSON)
			}
			if len(challenge.Completed) > 0 {
				t.Fatalf("expected 0 completed stages, got %d", len(challenge.Completed))
			}
			u.AddCompletedStage(tt.sessionID, "")
		})
	}
}
