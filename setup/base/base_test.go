package base_test

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"html/template"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/pitabwire/frame"

	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/httputil"
	basepkg "github.com/antinvestor/matrix/setup/base"
	"github.com/stretchr/testify/assert"
)

//go:embed static/*.gotmpl
var staticContent embed.FS

func TestLandingPage_Tcp(t *testing.T) {
	// generate the expected result
	tmpl := template.Must(template.ParseFS(staticContent, "static/*.gotmpl"))
	expectedRes := &bytes.Buffer{}
	err := tmpl.ExecuteTemplate(expectedRes, "index.gotmpl", map[string]string{
		"Version": internal.VersionString(),
	})
	assert.NoError(t, err)

	cfg, processCtx, closeRig := testrig.CreateConfig(t, test.DependancyOption{})
	defer closeRig()

	// Hack to get a free port to use in test
	s := httptest.NewServer(nil)
	s.Close()

	httpUrl, err := url.Parse(s.URL)
	cfg.Global.HttpServerPort = fmt.Sprintf(":%s", httpUrl.Port())

	ctx, service := frame.NewServiceWithContext(processCtx.Context(), "matrix tests",
		frame.Config(&cfg.Global))
	defer service.Stop(ctx)

	routers := httputil.NewRouters()

	assert.NoError(t, err)

	opt, err := basepkg.SetupHTTPOption(processCtx, cfg, routers)
	assert.NoError(t, err)

	go func(ctx context.Context, service *frame.Service, opt frame.Option) {
		service.Init(opt)
		err = service.Run(ctx, "")
	}(ctx, service, opt)
	time.Sleep(time.Millisecond * 10)

	// When hitting /, we should be redirected to /_matrix/static, which should contain the landing page
	req, err := http.NewRequest(http.MethodGet, s.URL, nil)
	assert.NoError(t, err)

	// do the request
	resp, err := s.Client().Do(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// read the response
	buf := &bytes.Buffer{}
	_, err = buf.ReadFrom(resp.Body)
	assert.NoError(t, err)

	// Using .String() for user friendly output
	assert.Equal(t, expectedRes.String(), buf.String(), "response mismatch")

}
