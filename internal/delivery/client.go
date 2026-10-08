package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/julimeimei/eventrelay-go/internal/domain"
)

type Result struct {
	StatusCode int
	Success    bool
}

type WebhookPayload struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type HTTPClient struct {
	client *http.Client
}

func NewHTTPClient(timeout time.Duration) *HTTPClient {
	return &HTTPClient{
		client: &http.Client{
			Timeout:   timeout,
			Transport: secureTransport(timeout),
		},
	}
}

func (c *HTTPClient) Deliver(ctx context.Context, event domain.Event) (Result, error) {
	payload := WebhookPayload{
		ID:      event.ID,
		Type:    event.Type,
		Payload: event.Payload,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return Result{}, fmt.Errorf("marshal delivery payload: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, event.TargetURL, bytes.NewReader(body))
	if err != nil {
		return Result{}, fmt.Errorf("create delivery request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "EventRelay/0.1")

	response, err := c.client.Do(request)
	if err != nil {
		return Result{}, errors.New("deliver webhook request failed")
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)

	return Result{
		StatusCode: response.StatusCode,
		Success:    response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices,
	}, nil
}

func secureTransport(timeout time.Duration) *http.Transport {
	shortTimeout := minDuration(timeout, 5*time.Second)
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   shortTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:      true,
		MaxIdleConns:           100,
		IdleConnTimeout:        90 * time.Second,
		TLSHandshakeTimeout:    shortTimeout,
		ResponseHeaderTimeout:  timeout,
		ExpectContinueTimeout:  time.Second,
		MaxResponseHeaderBytes: 1 << 20,
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a <= 0 {
		return b
	}
	if a < b {
		return a
	}
	return b
}
