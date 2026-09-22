package admission

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"

	"connectrpc.com/connect/v2"
	"github.com/caarlos0/env/v11"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"google.golang.org/protobuf/proto"

	"github.com/pbrpc/otel-testing/mocks/tracer"

	admissionpb "github.com/grpcd/protos/admission"
)

const (
	authenticate = "/auth.Authenticator/Authenticate"
	limit        = "/limits.Limiter/Check"
	target       = "/pkg.Service/Method"
	rewritten    = "/pkg.Service/Other"
)

// answer is what one call to the stub answers with.
type answer struct {
	response *admissionpb.AdmissionResponse
	err      error
}

// recorded is one call the stub received.
type recorded struct {
	procedure string
	request   *admissionpb.AdmissionRequest
}

// callerStub answers calls from answers in order, repeating the last, and
// records each call's procedure and request.
type callerStub struct {
	answers []answer

	mu    sync.Mutex
	calls []recorded
}

func (s *callerStub) CallUnary(_ context.Context, spec connect.Spec, req, res any) error {
	s.mu.Lock()
	n := len(s.calls)
	s.calls = append(s.calls, recorded{procedure: spec.Procedure, request: req.(*admissionpb.AdmissionRequest)})
	s.mu.Unlock()

	a := s.answers[min(n, len(s.answers)-1)]
	if a.err != nil {
		return a.err
	}

	if a.response != nil {
		proto.Merge(res.(proto.Message), a.response)
	}

	return nil
}

func (s *callerStub) received() []recorded {
	s.mu.Lock()
	defer s.mu.Unlock()

	return slices.Clone(s.calls)
}

// admit builds a chain over stub with the procedures and runs one request
// through it, answering with the request as the chain left it and the error.
func admit(t *testing.T, procedures []string, stub *callerStub, r *http.Request) (*http.Request, error) {
	t.Helper()

	chain := New(procedures, stub)

	return r, chain.Admit(t.Context(), r)
}

// hasAttribute reports whether attrs carries want.
func hasAttribute(attrs []attribute.KeyValue, want attribute.KeyValue) bool {
	for _, attr := range attrs {
		if attr.Key == want.Key && attr.Value == want.Value {
			return true
		}
	}

	return false
}

func newRequest(t *testing.T) *http.Request {
	t.Helper()

	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, target, nil)
	r.Header.Set("Authorization", "Bearer token")
	r.Header.Set("X-Principal-Id", "spoofed")

	return r
}

func TestAdmit(t *testing.T) {
	t.Run("admits everything with no admission services", func(t *testing.T) {
		stub := &callerStub{}

		if _, err := admit(t, nil, stub, newRequest(t)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(stub.received()) != 0 {
			t.Errorf("calls = %v, want none", stub.received())
		}
	})

	t.Run("sends the procedure, the headers, and the peer", func(t *testing.T) {
		stub := &callerStub{answers: []answer{{}}}

		r := newRequest(t)
		r.RemoteAddr = "203.0.113.7:4444"

		if _, err := admit(t, []string{authenticate}, stub, r); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		calls := stub.received()
		if len(calls) != 1 || calls[0].procedure != authenticate {
			t.Fatalf("calls = %v, want one to the authenticator", calls)
		}

		request := calls[0].request
		if request.GetProcedure() != target {
			t.Errorf("procedure = %q, want %q", request.GetProcedure(), target)
		}
		if got := request.GetHeaders()["Authorization"].GetValues(); !slices.Equal(got, []string{"Bearer token"}) {
			t.Errorf("authorization = %v, want the bearer", got)
		}
		if request.GetPeer().GetAddress() != r.RemoteAddr {
			t.Errorf("peer address = %q, want %q", request.GetPeer().GetAddress(), r.RemoteAddr)
		}
		if request.GetPeer().GetCertificate() != nil {
			t.Errorf("peer certificate = %v, want none without TLS", request.GetPeer().GetCertificate())
		}
	})

	t.Run("sends the client certificate the connection carried", func(t *testing.T) {
		stub := &callerStub{answers: []answer{{}}}

		r := newRequest(t)
		r.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{{Raw: []byte("leaf")}}}

		if _, err := admit(t, []string{authenticate}, stub, r); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if got := stub.received()[0].request.GetPeer().GetCertificate(); string(got) != "leaf" {
			t.Errorf("peer certificate = %q, want the leaf", got)
		}
	})

	t.Run("removes before it sets, each replacing every value", func(t *testing.T) {
		stub := &callerStub{answers: []answer{{response: &admissionpb.AdmissionResponse{
			Remove: []string{"x-principal-id", "Authorization"},
			Set: []*admissionpb.Header{
				{Name: "x-principal-id", Values: []string{"principal-1"}},
				{Name: "X-Tags", Values: []string{"a", "b"}},
			},
		}}}}

		r := newRequest(t)
		r.Header.Set("X-Tags", "old")

		r, err := admit(t, []string{authenticate}, stub, r)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if got := r.Header.Values("X-Principal-Id"); !slices.Equal(got, []string{"principal-1"}) {
			t.Errorf("principal = %v, want the one set", got)
		}
		if got := r.Header.Values("Authorization"); len(got) != 0 {
			t.Errorf("authorization = %v, want removed", got)
		}
		if got := r.Header.Values("X-Tags"); !slices.Equal(got, []string{"a", "b"}) {
			t.Errorf("tags = %v, want replaced", got)
		}
		if r.URL.Path != target {
			t.Errorf("path = %q, want unchanged", r.URL.Path)
		}
	})

	t.Run("hands each service the previous one's answer, procedure included", func(t *testing.T) {
		stub := &callerStub{answers: []answer{
			{response: &admissionpb.AdmissionResponse{
				Set:       []*admissionpb.Header{{Name: "X-Principal-Id", Values: []string{"principal-1"}}},
				Procedure: rewritten,
			}},
			{},
		}}

		r, err := admit(t, []string{authenticate, limit}, stub, newRequest(t))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		calls := stub.received()
		if len(calls) != 2 || calls[0].procedure != authenticate || calls[1].procedure != limit {
			t.Fatalf("calls = %v, want the authenticator then the limiter", calls)
		}

		second := calls[1].request
		if second.GetProcedure() != rewritten {
			t.Errorf("second procedure = %q, want the rewrite", second.GetProcedure())
		}
		if got := second.GetHeaders()["X-Principal-Id"].GetValues(); !slices.Equal(got, []string{"principal-1"}) {
			t.Errorf("second principal = %v, want the one the first set", got)
		}
		if r.URL.Path != rewritten {
			t.Errorf("path = %q, want the rewrite for the lookup", r.URL.Path)
		}
	})

	t.Run("answers the service's refusal and stops there", func(t *testing.T) {
		refusal := connect.NewError(connect.CodeUnauthenticated, "bad token")
		stub := &callerStub{answers: []answer{{err: refusal}}}

		chain := New([]string{authenticate, limit}, stub)

		err := chain.Admit(t.Context(), newRequest(t))
		if !errors.Is(err, refusal) {
			t.Fatalf("error = %v, want the refusal as returned", err)
		}
		if len(stub.received()) != 1 {
			t.Errorf("calls = %d, want the limiter never asked", len(stub.received()))
		}
	})

	t.Run("runs each call under a span naming the service, failed by its refusal", func(t *testing.T) {
		refusal := connect.NewError(connect.CodeUnauthenticated, "bad token")
		stub := &callerStub{answers: []answer{{}, {err: refusal}}}

		tt, ctx := tracer.New(t)
		defer tt.Shutdown(t)

		chain := New([]string{limit, authenticate}, stub)

		if err := chain.Admit(ctx, newRequest(t)); !errors.Is(err, refusal) {
			t.Fatalf("error = %v, want the refusal", err)
		}

		spans := tt.GetSpans()
		if len(spans) != 2 {
			t.Fatalf("recorded %d spans, want one per admission call", len(spans))
		}

		admitted, refused := spans[0], spans[1]

		if admitted.Name != "admit" || refused.Name != "admit" {
			t.Errorf("spans = %q, %q, want both named admit", admitted.Name, refused.Name)
		}
		if !hasAttribute(admitted.Attributes, semconv.RPCService("limits.Limiter")) ||
			!hasAttribute(admitted.Attributes, semconv.RPCMethod("Check")) {
			t.Errorf("attributes = %v, want the limiter named", admitted.Attributes)
		}
		if admitted.Status.Code != codes.Unset {
			t.Errorf("admitted status = %v, want unset", admitted.Status.Code)
		}
		if !hasAttribute(refused.Attributes, semconv.RPCMethod("Authenticate")) {
			t.Errorf("attributes = %v, want the authenticator named", refused.Attributes)
		}
		if !hasAttribute(refused.Attributes, semconv.RPCConnectRPCErrorCodeKey.String("unauthenticated")) {
			t.Errorf("attributes = %v, want the unauthenticated code", refused.Attributes)
		}
		if refused.Status.Code != codes.Error {
			t.Errorf("refused status = %v, want error", refused.Status.Code)
		}
	})

	t.Run("refuses as unavailable when a service cannot be reached", func(t *testing.T) {
		stub := &callerStub{answers: []answer{{err: errors.New("connection refused")}}}

		_, err := admit(t, []string{authenticate}, stub, newRequest(t))
		if connect.CodeOf(err) != connect.CodeUnavailable {
			t.Fatalf("error = %v, want unavailable", err)
		}
	})
}

func TestConfiguration(t *testing.T) {
	t.Run("reads the procedures in order", func(t *testing.T) {
		t.Setenv("GATEWAY_ADMISSION", authenticate+","+limit)

		configured, err := env.ParseAs[Configuration]()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if want := []string{authenticate, limit}; !slices.Equal(configured.Procedures, want) {
			t.Errorf("procedures = %v, want %v", configured.Procedures, want)
		}
	})

	t.Run("reads none when the variable is empty", func(t *testing.T) {
		t.Setenv("GATEWAY_ADMISSION", "")

		configured, err := env.ParseAs[Configuration]()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(configured.Procedures) != 0 {
			t.Errorf("procedures = %v, want none", configured.Procedures)
		}
	})
}
