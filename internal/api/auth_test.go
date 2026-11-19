package api

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMiddlewareAuthHeaderCompatibility(t *testing.T) {
	handler := middlewareAuth("admin", "secret", true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	valid := base64.StdEncoding.EncodeToString([]byte("admin:secret"))
	for _, tt := range []struct {
		header string
		status int
	}{
		{"Basic " + valid, http.StatusOK},
		{"basic " + valid, http.StatusOK},
		{"BASIC " + valid, http.StatusOK},
		{"Basic " + base64.StdEncoding.EncodeToString([]byte("admin:secret:extra")), http.StatusUnauthorized},
		{"Basic malformed", http.StatusUnauthorized},
		{"Bearer " + valid, http.StatusUnauthorized},
	} {
		r := httptest.NewRequest("GET", "/api/streams", nil)
		r.Header.Set("Authorization", tt.header)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		require.Equal(t, tt.status, w.Code, "header %q", tt.header)
	}
}

func TestMiddlewareAuth(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})

	tests := []struct {
		name       string
		remoteAddr string
		localAuth  bool
		user, pass string
		setAuth    bool
		status     int
	}{
		{"lan without auth", "192.168.1.10:5000", false, "", "", false, http.StatusUnauthorized},
		{"lan wrong password", "192.168.1.10:5000", false, "admin", "wrong", true, http.StatusUnauthorized},
		{"lan password prefix", "192.168.1.10:5000", false, "admin", "secre", true, http.StatusUnauthorized},
		{"lan wrong user", "192.168.1.10:5000", false, "root", "secret", true, http.StatusUnauthorized},
		{"lan valid", "192.168.1.10:5000", false, "admin", "secret", true, http.StatusOK},
		{"loopback skips auth", "127.0.0.1:5000", false, "", "", false, http.StatusOK},
		{"loopback with local_auth", "127.0.0.1:5000", true, "", "", false, http.StatusUnauthorized},
		{"loopback with local_auth valid", "127.0.0.1:5000", true, "admin", "secret", true, http.StatusOK},
	}

	handler := middlewareAuth("admin", "secret", false, ok)
	handlerLocal := middlewareAuth("admin", "secret", true, ok)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/api/streams", nil)
			r.RemoteAddr = tt.remoteAddr
			if tt.setAuth {
				r.SetBasicAuth(tt.user, tt.pass)
			}
			w := httptest.NewRecorder()
			if tt.localAuth {
				handlerLocal.ServeHTTP(w, r)
			} else {
				handler.ServeHTTP(w, r)
			}
			require.Equal(t, tt.status, w.Code)
		})
	}
}
