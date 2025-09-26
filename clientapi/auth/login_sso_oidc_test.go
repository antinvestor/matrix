package auth

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/test/testrig"
)

func TestOIDCIdentityProviderAuthorizationURL(t *testing.T) {
	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/discovery", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"authorization_endpoint":"http://oidc.example.com/authorize","token_endpoint":"http://oidc.example.com/token","userinfo_endpoint":"http://oidc.example.com/userinfo","issuer":"http://oidc.example.com/"}`))
	})

	s := httptest.NewServer(mux)
	defer s.Close()

	idp := newSSOIdentityProvider("example.com", &config.IdentityProvider{
		ClientID:     "aclientid",
		DiscoveryURL: s.URL + "/discovery",
	}, s.Client())

	got, err := idp.AuthorizationURL(ctx, "https://matrix.example.com/continue", "anonce", "codeVerifier")
	if err != nil {
		t.Fatalf("AuthorizationURL failed: %v", err)
	}

	parsedURL, err := url.Parse(got)
	if err != nil {
		t.Fatalf("Parse obtained url failed: %v", err)
	}

	if parsedURL.Scheme != "http" {
		t.Errorf("Expected scheme 'http', got '%s'", parsedURL.Scheme)
	}
	if parsedURL.Host != "oidc.example.com" {
		t.Errorf("Expected host 'oidc.example.com', got '%s'", parsedURL.Host)
	}

	// Verify the path
	if parsedURL.Path != "/authorize" {
		t.Errorf("Expected path '/authorize', got '%s'", parsedURL.Path)
	}

	// Verify query parameters
	expectedParams := map[string]string{
		"client_id":             "aclientid",
		"redirect_uri":          "https://matrix.example.com/continue",
		"response_type":         "code",
		"scope":                 "openid offline_access profile contact",
		"state":                 "anonce",
		"code_challenge_method": "S256",
		"access_type":           "offline",
	}

	queryParams := parsedURL.Query()
	for key, expectedValue := range expectedParams {
		actualValue := queryParams.Get(key)
		if actualValue != expectedValue {
			t.Errorf("Expected query param '%s' to have value '%s', got '%s'", key, expectedValue, actualValue)
		}
	}
}

func TestOIDCIdentityProviderProcessCallback(t *testing.T) {
	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)

	const callbackURL = "https://matrix.example.com/continue"

	tsts := []struct {
		Name  string
		Query url.Values

		Want         *CallbackResult
		WantTokenReq url.Values
	}{
		{
			Name: "gotEverything",
			Query: url.Values{
				"code":  []string{"acode"},
				"state": []string{"anonce"},
			},

			Want: &CallbackResult{
				Identifier: UserIdentifier{
					Issuer:  "http://oidc.example.com/",
					Subject: "asub",
				},
				DisplayName:     "aname",
				SuggestedUserID: "@asub:example.com",
				Contacts:        []string{"auser"},
			},
		},
	}
	for _, tst := range tsts {
		t.Run(tst.Name, func(t *testing.T) {
			mux := http.NewServeMux()
			var sURL string
			mux.HandleFunc("/discovery", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprintf(w, `{"authorization_endpoint":"%s/authorize","token_endpoint":"%s/token","userinfo_endpoint":"%s/userinfo","issuer":"http://oidc.example.com/"}`,
					sURL, sURL, sURL)
			})
			mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"access_token":"atoken", "token_type":"Bearer"}`))
			})
			mux.HandleFunc("/userinfo", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"sub":"asub", "name":"aname", "contacts":[{"detail":"auser"}]}`))
			})

			s := httptest.NewServer(mux)
			defer s.Close()

			sURL = s.URL
			idp := newSSOIdentityProvider("example.com", &config.IdentityProvider{
				ClientID:     "aclientid",
				DiscoveryURL: sURL + "/discovery",
			}, s.Client())

			got, err := idp.ProcessCallback(ctx, callbackURL, "anonce", "codeVerifier", tst.Query)
			if err != nil {
				t.Fatalf("ProcessCallback failed: %v", err)
			}

			got.Token = nil
			if !reflect.DeepEqual(got, tst.Want) {
				t.Errorf("ProcessCallback: got %+v, want %+v", got, tst.Want)
			}
		})
	}
}

func TestOAuth2IdentityProviderAuthorizationURL(t *testing.T) {
	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)

	idp := &oidcIdentityProvider{
		cfg: &config.IdentityProvider{
			ClientID: "aclientid",
		},
		hc:                http.DefaultClient,
		exp:               time.Now().Add(2 * time.Minute),
		resetOauth2Config: true,
		disc: &oidcDiscovery{

			Issuer:                "https://oauth2.example.com",
			AuthorizationEndpoint: "https://oauth2.example.com/authorize",
			TokenEndpoint:         "https://oauth2.example.com/token",
			UserinfoEndpoint:      "https://oauth2.example.com/profile",
			ScopesSupported:       []string{},
		},
	}

	got, err := idp.AuthorizationURL(ctx, "https://matrix.example.com/continue", "anonce", "codeVerifier")
	if err != nil {
		t.Fatalf("AuthorizationURL failed: %v", err)
	}

	if want := "https://oauth2.example.com/authorize?access_type=offline&client_id=aclientid&code_challenge=N1E4yRMD7xixn_oFyO_W3htYN3rY7-HMDKJe6z6r928&code_challenge_method=S256&redirect_uri=https%3A%2F%2Fmatrix.example.com%2Fcontinue&response_type=code&scope=&state=anonce"; got != want {
		t.Errorf("AuthorizationURL: got %q, want %q", got, want)
	}
}

func TestOAuth2IdentityProviderProcessCallback(t *testing.T) {
	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)

	const callbackURL = "https://matrix.example.com/continue"

	tsts := []struct {
		Name  string
		Query url.Values

		Want         *CallbackResult
		WantTokenReq url.Values
	}{
		{
			Name: "gotEverything",
			Query: url.Values{
				"code":  []string{"acode"},
				"state": []string{"anonce"},
			},

			Want: &CallbackResult{
				Identifier: UserIdentifier{
					Issuer:  "https://oauth2.example.com",
					Subject: "asub",
				},
				DisplayName:     "aname",
				SuggestedUserID: "@asub:example.com",
				Contacts:        []string{"auser"},
			},
		},
	}
	for _, tst := range tsts {
		t.Run(tst.Name, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"access_token":"atoken", "token_type":"Bearer"}`))
			})
			mux.HandleFunc("/userinfo", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"sub":"asub", "name":"aname", "contacts":[{"detail":"auser"}]}`))
			})

			s := httptest.NewServer(mux)
			defer s.Close()

			idp := &oidcIdentityProvider{
				cfg: &config.IdentityProvider{
					ID:           "anid",
					ClientID:     "aclientid",
					ClientSecret: "aclientsecret",
				},
				serverName:        "example.com",
				hc:                s.Client(),
				exp:               time.Now().Add(2 * time.Minute),
				resetOauth2Config: true,

				userInfoURL: s.URL + "/userinfo",

				disc: &oidcDiscovery{
					Issuer:                "https://oauth2.example.com",
					AuthorizationEndpoint: s.URL + "/authorize",
					TokenEndpoint:         s.URL + "/token",
					UserinfoEndpoint:      s.URL + "/userinfo",
					ScopesSupported:       []string{},
				},
			}

			got, err := idp.ProcessCallback(ctx, callbackURL, "anonce", "codeVerifier", tst.Query)
			if err != nil {
				t.Fatalf("ProcessCallback failed: %v", err)
			}

			got.Token = nil

			if !reflect.DeepEqual(got, tst.Want) {
				t.Errorf("ProcessCallback: got %+v, want %+v", got, tst.Want)
			}
		})
	}
}

func TestOAuth2IdentityProviderGetUserInfo(t *testing.T) {
	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)

	mux := http.NewServeMux()
	var gotHeader http.Header
	mux.HandleFunc("/userinfo", func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"sub":"asub", "name":"aname", "contacts":[{"detail":"auser"}]}`))
	})

	s := httptest.NewServer(mux)
	defer s.Close()

	idp := &oidcIdentityProvider{
		cfg: &config.IdentityProvider{
			ID:           "anid",
			ClientID:     "aclientid",
			ClientSecret: "aclientsecret",
		},
		hc: s.Client(),

		userInfoURL: s.URL + "/userinfo",

		responseMimeType: "application/json",
	}

	gotSub, gotName, contacts, err := idp.getUserInfo(ctx, "atoken")
	if err != nil {
		t.Fatalf("getUserInfo failed: %v", err)
	}

	if want := "asub"; gotSub != want {
		t.Errorf("getUserInfo subject: got %q, want %q", gotSub, want)
	}
	if want := "aname"; gotName != want {
		t.Errorf("getUserInfo displayName: got %q, want %q", gotName, want)
	}
	if want := []string{"auser"}; !slices.Equal(contacts, want) {
		t.Errorf("getUserInfo contacts: got %q, want %q", contacts, want)
	}

	gotHeader.Del("Accept-Encoding")
	gotHeader.Del("User-Agent")
	wantHeader := http.Header{
		"Accept":        []string{"application/json"},
		"Authorization": []string{"Bearer atoken"},
	}
	if !reflect.DeepEqual(gotHeader, wantHeader) {
		t.Errorf("getUserInfo header: got %+v, want %+v", gotHeader, wantHeader)
	}
}
