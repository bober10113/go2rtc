package nest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestNestStatusErrorSafeDetails(t *testing.T) {
	tests := []struct {
		name, body, want string
	}{
		{"unavailable", `{"error":{"code":400,"status":"FAILED_PRECONDITION","message":"The camera is not available for streaming."}}`, " rpc=FAILED_PRECONDITION reason=camera_unavailable"},
		{"unknown message", `{"error":{"status":"INVALID_ARGUMENT","message":"invalid SDP for private-device-secret","details":[{"token":"private-token"}]}}`, " rpc=INVALID_ARGUMENT"},
		{"untrusted status", `{"error":{"status":"private-device-secret","message":"private-token"}}`, ""},
		{"malformed", `<html>private-token</html>`, ""},
		{"oauth", `{"error":"invalid_grant","error_description":"private-token"}`, ""},
		{"oversized", `{"error":{"status":"INVALID_ARGUMENT","message":"` + strings.Repeat("a", 16*1024) + `"}}`, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := strings.NewReader(tt.body)
			err := newNestStatusError("GenerateWebRtcStream", &http.Response{
				StatusCode: http.StatusBadRequest, Status: "400 Bad Request", Body: io.NopCloser(body),
			})
			if got, want := err.Error(), "nest: wrong status: 400 Bad Request"+tt.want; got != want {
				t.Fatalf("error = %q, want %q", got, want)
			}
			if read := len(tt.body) - body.Len(); read > 16*1024+1 {
				t.Fatalf("read %d bytes from error body", read)
			}
			wrapped := fmt.Errorf("request failed: %w", err)
			if nestStatusCode(wrapped) != http.StatusBadRequest || !nestTerminalExtendStatus(wrapped) {
				t.Fatal("HTTP error classification changed")
			}
			var typed *nestStatusError
			if !errors.As(err, &typed) || typed.Command != "GenerateWebRtcStream" {
				t.Fatal("lost command context")
			}
		})
	}
}

func TestNestStatusErrorWithoutBody(t *testing.T) {
	err := newNestStatusError("GenerateWebRtcStream", &http.Response{StatusCode: 429, Status: "429 Too Many Requests"})
	if !nestRateLimitStatus(err) || err.Error() != "nest: wrong status: 429 Too Many Requests" {
		t.Fatalf("unexpected error: %v", err)
	}
}

type testNestTransport struct {
	roundTrip func(*http.Request) (*http.Response, error)
	closed    atomic.Int32
}

func (t *testNestTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return t.roundTrip(req)
}

func (t *testNestTransport) CloseIdleConnections() { t.closed.Add(1) }

func TestNestCommandQueueExpiresWithoutSendingOrLeakingSlot(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	transport := &testNestTransport{roundTrip: func(*http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("ok"))}, nil
	}}
	client := &http.Client{Transport: transport, Timeout: 20 * time.Millisecond}
	req, _ := http.NewRequest("POST", "http://example.invalid/test", nil)
	done := make(chan error, 1)
	go func() {
		res, err := doNestRequest(client, req, "Test", "test-one", 1)
		if res != nil {
			res.Body.Close()
		}
		done <- err
	}()
	<-entered
	_, err := doNestRequest(client, req, "Test", "test-two", 1)
	close(release)
	<-done
	if !errors.Is(err, context.DeadlineExceeded) || calls.Load() != 1 {
		t.Fatalf("queued request err=%v calls=%d", err, calls.Load())
	}
	res, err := doNestRequest(client, req, "Test", "test-three", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil || string(body) != "ok" || calls.Load() != 2 {
		t.Fatalf("later request: body=%q err=%v calls=%d", body, err, calls.Load())
	}
}

func TestNestCommandCancellationDoesNotSend(t *testing.T) {
	transport := &testNestTransport{roundTrip: func(*http.Request) (*http.Response, error) {
		t.Fatal("sent canceled command")
		return nil, nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", "http://example.invalid/test", nil)
	_, err := doNestRequest(&http.Client{Transport: transport}, req, "Test", "test", 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want cancellation", err)
	}
}

func TestNestTransportFailureClearsOnlyIdleConnections(t *testing.T) {
	failed := errors.New("test broken connection")
	transport := &testNestTransport{roundTrip: func(*http.Request) (*http.Response, error) {
		return nil, failed
	}}
	req, _ := http.NewRequest("POST", "http://example.invalid/test", nil)
	_, err := doNestRequest(&http.Client{Transport: transport}, req, "Test", "test", 1)
	if !errors.Is(err, failed) || transport.closed.Load() != 1 {
		t.Fatalf("err=%v idle-close count=%d", err, transport.closed.Load())
	}
	client := newNestHTTPClient(time.Second)
	if client.Transport == http.DefaultTransport || client.Transport != nestHTTPTransport {
		t.Fatal("Nest is not using its own connection pool")
	}
}
