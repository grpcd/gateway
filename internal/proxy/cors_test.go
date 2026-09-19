package proxy

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const origin = "https://app.example"

// echo answers 204 and records that it ran.
func echo(ran *bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		*ran = true

		w.WriteHeader(http.StatusNoContent)
	})
}

func TestCORS(t *testing.T) {
	policy := CORS(CORSConfiguration{AllowedOrigins: []string{origin}, AllowedHeaders: []string{"Authorization"}})

	t.Run("answers a preflight without reaching the handler", func(t *testing.T) {
		ran := false

		request := httptest.NewRequestWithContext(t.Context(), http.MethodOptions, procedure, nil)
		request.Header.Set("Origin", origin)
		request.Header.Set("Access-Control-Request-Method", http.MethodPost)
		// The form a browser sends: lowercase, sorted, no spaces.
		request.Header.Set("Access-Control-Request-Headers", "authorization,connect-protocol-version,content-type")

		recorder := httptest.NewRecorder()
		policy(procedure, echo(&ran)).ServeHTTP(recorder, request)

		if ran {
			t.Error("the preflight reached the handler")
		}
		if got := recorder.Header().Get("Access-Control-Allow-Origin"); got != origin {
			t.Errorf("allow origin = %q, want %q", got, origin)
		}

		allowed := strings.ToLower(recorder.Header().Get("Access-Control-Allow-Headers"))
		for _, want := range []string{"authorization", "connect-protocol-version", "content-type"} {
			if !strings.Contains(allowed, want) {
				t.Errorf("allow headers = %q, want %s", allowed, want)
			}
		}
	})

	t.Run("marks an actual response and passes it on", func(t *testing.T) {
		ran := false

		request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, procedure, nil)
		request.Header.Set("Origin", origin)

		recorder := httptest.NewRecorder()
		policy(procedure, echo(&ran)).ServeHTTP(recorder, request)

		if !ran {
			t.Fatal("the request did not reach the handler")
		}
		if got := recorder.Header().Get("Access-Control-Allow-Origin"); got != origin {
			t.Errorf("allow origin = %q, want %q", got, origin)
		}
		if got := recorder.Header().Get("Access-Control-Expose-Headers"); !strings.Contains(strings.ToLower(got), "grpc-status") {
			t.Errorf("expose headers = %q, want the protocols' response headers", got)
		}
	})

	t.Run("refuses an origin not named", func(t *testing.T) {
		ran := false

		request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, procedure, nil)
		request.Header.Set("Origin", "https://elsewhere.example")

		recorder := httptest.NewRecorder()
		policy(procedure, echo(&ran)).ServeHTTP(recorder, request)

		if got := recorder.Header().Get("Access-Control-Allow-Origin"); got != "" {
			t.Errorf("allow origin = %q, want none", got)
		}
	})
}
