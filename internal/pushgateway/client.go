package pushgateway

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/antinvestor/matrix/internal"
	"github.com/pitabwire/frame"
	"github.com/pitabwire/frame/telemetry"
)

type httpClient struct {
	hc     *http.Client
	tracer frame.Tracer
}

// NewHTTPClient creates a new Push Gateway client.
func NewHTTPClient(disableTLSValidation bool) Client {
	hc := frame.NewHTTPClient(
		frame.WithHTTPTimeout(30*time.Second),
		frame.WithHTTPTransport(
			&http.Transport{
				DisableKeepAlives: true,
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: disableTLSValidation,
				},
				Proxy: http.ProxyFromEnvironment,
			}),
	)

	return &httpClient{hc: hc, tracer: telemetry.NewTracer("pushgateway")}
}

func (h *httpClient) Notify(ctx context.Context, url string, req *NotifyRequest, resp *NotifyResponse) error {
	var err error

	ctx, span := h.tracer.Start(ctx, "Notify")
	defer h.tracer.End(ctx, span, err)

	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	hreq.Header.Set("Content-Type", "application/json")

	hresp, err := h.hc.Do(hreq)
	if err != nil {
		return err
	}

	defer internal.CloseAndLogIfError(ctx, hresp.Body, "failed to close response body")

	if hresp.StatusCode == http.StatusOK {
		return json.NewDecoder(hresp.Body).Decode(resp)
	}

	var errorBody struct {
		Message string `json:"message"`
	}
	if err := json.NewDecoder(hresp.Body).Decode(&errorBody); err == nil {
		return fmt.Errorf("push gateway: %d from %s: %s", hresp.StatusCode, url, errorBody.Message)
	}
	return fmt.Errorf("push gateway: %d from %s", hresp.StatusCode, url)
}
