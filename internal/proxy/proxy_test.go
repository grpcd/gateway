package proxy

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"

	"github.com/pbrpc/otel-testing/mocks/tracer"
	"github.com/pbrpc/testing/mocks/responsewriter"
	"github.com/pbrpc/testing/mocks/roundtripper"

	"github.com/grpcd/connect-client/discover"
)

const procedure = "/pkg.Service/Method"

var errUnreachable = errors.New("connection refused")

// hasAttribute reports whether attrs carries want.
func hasAttribute(attrs []attribute.KeyValue, want attribute.KeyValue) bool {
	for _, attr := range attrs {
		if attr.Key == want.Key && attr.Value == want.Value {
			return true
		}
	}

	return false
}

// spanNamed answers with the recorded span called name, failing the test
// when there is not exactly one.
func spanNamed(t *testing.T, spans tracetest.SpanStubs, name string) tracetest.SpanStub {
	t.Helper()

	var found []tracetest.SpanStub

	for _, span := range spans {
		if span.Name == name {
			found = append(found, span)
		}
	}

	if len(found) != 1 {
		t.Fatalf("recorded %d spans named %q, want 1", len(found), name)
	}

	return found[0]
}

// assertRPC fails the test unless span names the procedure.
func assertRPC(t *testing.T, span tracetest.SpanStub) {
	t.Helper()

	for _, want := range []attribute.KeyValue{
		semconv.RPCSystemConnectRPC,
		semconv.RPCService("pkg.Service"),
		semconv.RPCMethod("Method"),
	} {
		if !hasAttribute(span.Attributes, want) {
			t.Errorf("%s attributes = %v, want %v", span.Name, span.Attributes, want)
		}
	}
}

// assertFailed fails the test unless span carries code and an error status.
func assertFailed(t *testing.T, span tracetest.SpanStub, code string) {
	t.Helper()

	if !hasAttribute(span.Attributes, semconv.RPCConnectRPCErrorCodeKey.String(code)) {
		t.Errorf("%s attributes = %v, want the %s code", span.Name, span.Attributes, code)
	}
	if span.Status.Code != codes.Error {
		t.Errorf("%s status = %v, want error", span.Name, span.Status.Code)
	}
}

// replica stands in for the discovery transport with a replica behind it
// that answers every request, recording what it was handed.
func replica() *roundtripper.Recorder {
	return roundtripper.Record(roundtripper.Respond(
		http.StatusOK, http.Header{"Content-Type": {"application/proto"}, "X-Replica": {"replica-a"}}, "reply",
	))
}

// failing stands in for the discovery transport failing every request with
// err, recording what it was handed.
func failing(err error) *roundtripper.Recorder {
	return roundtripper.Record(roundtripper.Fail(err))
}

// received answers with the one request rec carried and its body, or nil
// when nothing was forwarded.
func received(rec *roundtripper.Recorder) (*http.Request, string) {
	sent := rec.Sent()
	if len(sent) == 0 {
		return nil, ""
	}

	return sent[0].Request, sent[0].Body
}

// admitAll admits every request untouched.
func admitAll(context.Context, *http.Request) error { return nil }

// serve runs one request through a handler over transport with admit, and
// answers with the recorded response.
func serve(t *testing.T, transport http.RoundTripper, admit Admit, request *http.Request) *httptest.ResponseRecorder {
	t.Helper()

	recorder := httptest.NewRecorder()
	New(transport, admit, slog.New(slog.DiscardHandler)).ServeHTTP(recorder, request)

	return recorder
}

// newRequest builds a Connect-protocol call to procedure with a body.
func newRequest(t *testing.T, contentType string) *http.Request {
	t.Helper()

	return newRequestWithContext(t.Context(), contentType)
}

// newRequestWithContext builds a Connect-protocol call to procedure with a
// body, arriving on ctx.
func newRequestWithContext(ctx context.Context, contentType string) *http.Request {
	request := httptest.NewRequestWithContext(ctx, http.MethodPost, procedure, strings.NewReader("message"))
	request.Header.Set("Content-Type", contentType)
	request.Header.Set("Authorization", "Bearer token")

	return request
}

// tracedRequest builds a request arriving under a recorded span, the way the
// HTTP layer starts one per request, and answers with the recorder.
func tracedRequest(t *testing.T, contentType string) (*tracer.Mock, *http.Request) {
	t.Helper()

	tt, ctx := tracer.New(t)
	t.Cleanup(func() { tt.Shutdown(t) })

	return tt, newRequestWithContext(ctx, contentType)
}

func TestServeHTTP(t *testing.T) {
	t.Run("forwards the request as it arrived, addressed by the grpcd scheme", func(t *testing.T) {
		transport := replica()

		request := newRequest(t, "application/json")
		request.Host = "api.example"

		recorder := serve(t, transport, admitAll, request)

		seen, body := received(transport)
		if seen == nil {
			t.Fatal("nothing forwarded")
		}
		if seen.URL.Scheme != discover.Scheme || seen.URL.Host != "" || seen.URL.Path != procedure {
			t.Errorf("forwarded to %s, want %s%s", seen.URL, discover.BaseURL, strings.TrimPrefix(procedure, "/"))
		}
		if seen.Host != "api.example" {
			t.Errorf("host = %q, want the client's", seen.Host)
		}
		if seen.Header.Get("Authorization") != "Bearer token" {
			t.Errorf("authorization = %q, want passed through", seen.Header.Get("Authorization"))
		}
		if body != "message" {
			t.Errorf("body = %q, want the client's", body)
		}

		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d, want the replica's 200", recorder.Code)
		}
		if recorder.Header().Get("X-Replica") != "replica-a" {
			t.Errorf("headers = %v, want the replica's", recorder.Header())
		}
		if recorder.Body.String() != "reply" {
			t.Errorf("body = %q, want the replica's", recorder.Body.String())
		}
		if !recorder.Flushed {
			t.Error("response was not flushed as it was written")
		}
	})

	t.Run("forwards what admission changed", func(t *testing.T) {
		transport := replica()

		admit := func(_ context.Context, r *http.Request) error {
			r.Header.Del("Authorization")
			r.Header.Set("X-Principal-Id", "principal-1")
			r.URL.Path = "/pkg.Service/Other"

			return nil
		}

		serve(t, transport, admit, newRequest(t, "application/json"))

		seen, _ := received(transport)
		if seen.Header.Get("Authorization") != "" || seen.Header.Get("X-Principal-Id") != "principal-1" {
			t.Errorf("headers = %v, want admission's", seen.Header)
		}
		if seen.URL.Path != "/pkg.Service/Other" {
			t.Errorf("path = %q, want the rewrite looked up", seen.URL.Path)
		}
	})

	t.Run("answers a refusal in the client's protocol and forwards nothing", func(t *testing.T) {
		refusal := connect.NewError(connect.CodeUnauthenticated, "bad token")
		refuse := func(context.Context, *http.Request) error { return refusal }

		cases := map[string]struct {
			contentType string
			status      int
			check       func(*httptest.ResponseRecorder) bool
		}{
			"connect": {
				contentType: "application/json",
				status:      http.StatusUnauthorized,
				check: func(r *httptest.ResponseRecorder) bool {
					return strings.Contains(r.Body.String(), `"unauthenticated"`)
				},
			},
			"grpc": {
				contentType: "application/grpc",
				status:      http.StatusOK,
				check: func(r *httptest.ResponseRecorder) bool {
					return r.Header().Get("Grpc-Status") == "16"
				},
			},
			"grpc-web": {
				contentType: "application/grpc-web",
				status:      http.StatusOK,
				check: func(r *httptest.ResponseRecorder) bool {
					return r.Header().Get("Grpc-Status") == "16"
				},
			},
		}

		for name, c := range cases {
			t.Run(name, func(t *testing.T) {
				transport := replica()

				recorder := serve(t, transport, refuse, newRequest(t, c.contentType))

				if recorder.Code != c.status {
					t.Errorf("status = %d, want %d", recorder.Code, c.status)
				}
				if !c.check(recorder) {
					t.Errorf("response = %d %v %q, want the refusal framed for %s", recorder.Code, recorder.Header(), recorder.Body.String(), name)
				}
				if seen, _ := received(transport); seen != nil {
					t.Error("a refused request was forwarded")
				}
			})
		}
	})

	t.Run("answers unavailable when the replica cannot be reached", func(t *testing.T) {
		transport := failing(errUnreachable)

		recorder := serve(t, transport, admitAll, newRequest(t, "application/json"))

		if recorder.Code != http.StatusServiceUnavailable {
			t.Errorf("status = %d, want 503", recorder.Code)
		}
		if !strings.Contains(recorder.Body.String(), `"unavailable"`) {
			t.Errorf("body = %q, want unavailable", recorder.Body.String())
		}
	})

	t.Run("answers unimplemented for a path naming no procedure", func(t *testing.T) {
		transport := failing(discover.ErrNoProcedure)

		request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/not-a-procedure", nil)
		request.Header.Set("Content-Type", "application/json")

		recorder := serve(t, transport, admitAll, request)

		if recorder.Code != http.StatusNotImplemented {
			t.Errorf("status = %d, want 501", recorder.Code)
		}
		if !strings.Contains(recorder.Body.String(), `"unimplemented"`) {
			t.Errorf("body = %q, want unimplemented", recorder.Body.String())
		}
	})

	t.Run("survives a client that stopped reading before the error was written", func(t *testing.T) {
		transport := failing(errUnreachable)

		w := responsewriter.NewBroken()
		New(transport, admitAll, slog.New(slog.DiscardHandler)).ServeHTTP(w, newRequest(t, "application/json"))

		if w.Status != http.StatusServiceUnavailable {
			t.Errorf("status = %d, want 503 attempted", w.Status)
		}
	})

	t.Run("names the RPC on the request's span and forwards under a span of its own", func(t *testing.T) {
		tt, request := tracedRequest(t, "application/json")

		serve(t, replica(), admitAll, request)
		tt.EndSpan()

		spans := tt.GetSpans()

		forward := spanNamed(t, spans, "forward")
		assertRPC(t, forward)
		if forward.Status.Code != codes.Unset {
			t.Errorf("forward status = %v, want unset", forward.Status.Code)
		}

		server := spanNamed(t, spans, "test-span")
		assertRPC(t, server)
		if forward.Parent.SpanID() != server.SpanContext.SpanID() {
			t.Error("forward span is not under the request's span")
		}
	})

	t.Run("fails the forward span with the code a failed forward answers", func(t *testing.T) {
		cases := map[string]struct {
			err  error
			code string
		}{
			"unreachable":  {err: errUnreachable, code: "unavailable"},
			"no procedure": {err: discover.ErrNoProcedure, code: "unimplemented"},
		}

		for name, c := range cases {
			t.Run(name, func(t *testing.T) {
				tt, request := tracedRequest(t, "application/json")

				serve(t, failing(c.err), admitAll, request)
				tt.EndSpan()

				spans := tt.GetSpans()

				assertFailed(t, spanNamed(t, spans, "forward"), c.code)

				// The request's own span answers with what the HTTP layer
				// observes; the forward is what failed.
				if server := spanNamed(t, spans, "test-span"); server.Status.Code != codes.Unset {
					t.Errorf("request span status = %v, want unset", server.Status.Code)
				}
			})
		}
	})

	t.Run("fails the request's span with a refusal and starts no forward", func(t *testing.T) {
		refuse := func(context.Context, *http.Request) error {
			return connect.NewError(connect.CodeUnauthenticated, "bad token")
		}

		tt, request := tracedRequest(t, "application/json")

		serve(t, replica(), refuse, request)
		tt.EndSpan()

		spans := tt.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("recorded %d spans, want the request's alone", len(spans))
		}

		assertRPC(t, spans[0])
		assertFailed(t, spans[0], "unauthenticated")
	})
}

func TestNew(t *testing.T) {
	t.Run("substitutes a logger", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		New(failing(errUnreachable), admitAll, nil).ServeHTTP(recorder, newRequest(t, "application/json"))

		if recorder.Code != http.StatusServiceUnavailable {
			t.Errorf("status = %d, want 503", recorder.Code)
		}
	})
}
