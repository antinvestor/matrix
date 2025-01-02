package auth

import (
	"context"
	"net/url"
	"reflect"
	"testing"

	"github.com/antinvestor/matrix/setup/config"
)

func TestNewAuthenticator(t *testing.T) {
	a := NewAuthenticator(&config.LoginSSO{
		Providers: []config.IdentityProvider{
			{
				ClientID: "aclientid",
			},
			{
				ClientID:     "aclientid",
				DiscoveryURL: "http://oidc.example.com/discovery",
			},
		},
	})
	if a == nil {
		t.Fatalf("NewAuthenticator failed to be instantiated")
	}
}

func TestAuthenticator(t *testing.T) {
	ctx := context.Background()

	var idp fakeIdentityProvider
	a := Authenticator{
		providers: map[string]ssoIdentityProvider{
			"fake": &idp,
		},
	}

	t.Run("authorizationURL", func(t *testing.T) {
		got, err := a.AuthorizationURL(ctx, "fake", "http://matrix.example.com/continue", "anonce", "codeVerifier")
		if err != nil {
			t.Fatalf("AuthorizationURL failed: %v", err)
		}
		if want := "aurl"; got != want {
			t.Error("AuthorizationURL: got %q, want %q", got, want)
		}
	})

	t.Run("processCallback", func(t *testing.T) {
		got, err := a.ProcessCallback(ctx, "fake", "http://matrix.example.com/continue", "anonce", "codeVerifier", url.Values{})
		if err != nil {
			t.Fatalf("ProcessCallback failed: %v", err)
		}
		if want := (&CallbackResult{DisplayName: "aname"}); !reflect.DeepEqual(got, want) {
			t.Error("ProcessCallback: got %+v, want %+v", got, want)
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
