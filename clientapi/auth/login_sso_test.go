package auth

import (
	"context"
	"net/url"
	"reflect"
	"testing"

	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/test/testrig"
	"golang.org/x/oauth2"
)

func TestNewAuthenticator(t *testing.T) {
	a := NewAuthenticator("example.com", &config.LoginSSO{
		Providers: []config.IdentityProvider{
			{
				ClientID: "aclientid",
			},
			{
				ClientID:     "aclientid",
				DiscoveryURL: "http://oidc.example.com/discovery",
			},
		},
	}, nil)
	if a == nil {
		t.Fatalf("NewAuthenticator failed to be instantiated")
	}
}

func TestAuthenticator(t *testing.T) {
	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)

	var idp fakeIdentityProvider

	a := Authenticator{}

	a.providers.Store("fake", &idp)

	t.Run("authorizationURL", func(t *testing.T) {
		got, err := a.AuthorizationURL(ctx, "fake", "http://matrix.example.com/continue", "anonce", "codeVerifier")
		if err != nil {
			t.Fatalf("AuthorizationURL failed: %v", err)
		}
		if want := "aurl"; got != want {
			t.Errorf("AuthorizationURL: got %q, want %q", got, want)
		}
	})

	t.Run("processCallback", func(t *testing.T) {
		got, err := a.ProcessCallback(ctx, "fake", "http://matrix.example.com/continue", "anonce", "codeVerifier", url.Values{})
		if err != nil {
			t.Fatalf("ProcessCallback failed: %v", err)
		}
		if want := (&CallbackResult{DisplayName: "aname"}); !reflect.DeepEqual(got, want) {
			t.Errorf("ProcessCallback: got %+v, want %+v", got, want)
		}
	})
}

type fakeIdentityProvider struct{}

func (idp *fakeIdentityProvider) AuthorizationURL(ctx context.Context, callbackURL, nonce, codeVerifier string) (string, error) {
	return "aurl", nil
}

func (idp *fakeIdentityProvider) ProcessCallback(ctx context.Context, callbackURL, nonce, codeVerifier string, query url.Values) (*CallbackResult, error) {
	return &CallbackResult{DisplayName: "aname"}, nil
}

func (idp *fakeIdentityProvider) RefreshToken(ctx context.Context, refreshToken string) (*oauth2.Token, error) {
	return &oauth2.Token{AccessToken: refreshToken, RefreshToken: refreshToken}, nil
}
