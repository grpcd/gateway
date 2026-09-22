// Package admission consults the admission services a request passes through
// before it is forwarded.
package admission

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"slices"

	"connectrpc.com/connect/v2"
	"go.opentelemetry.io/otel/trace"

	"git.sonicoriginal.software/logger"

	admissionpb "github.com/grpcd/protos/admission"
	connectotel "github.com/pbrpc/connect-otel"
)

// tracerName is this package's instrumentation scope.
const tracerName = "grpcd/gateway/internal/admission"

// Caller makes the unary call to an admission service. A *connect.Client
// built against discover.BaseURL is one: the procedure in the Spec is what
// routes the call to the replica held for it.
type Caller interface {
	CallUnary(ctx context.Context, spec connect.Spec, req, res any) error
}

// Chain is the admission services a request passes through, in order.
type Chain struct {
	steps  []connect.Spec
	caller Caller
}

// New builds the chain for procedures, in the order given, called over
// caller. An empty list admits everything.
func New(procedures []string, caller Caller) *Chain {
	steps := make([]connect.Spec, 0, len(procedures))
	for _, procedure := range procedures {
		spec := connect.Spec{StreamType: connect.StreamTypeUnary, Procedure: procedure}
		steps = append(steps, spec)
	}

	return &Chain{steps: steps, caller: caller}
}

// Configuration is the admission chain's: the procedures a request passes
// through, in order, comma-separated with no surrounding whitespace. Unset
// or empty is no admission.
type Configuration struct {
	Procedures []string `env:"GATEWAY_ADMISSION"`
}

// Admit consults each admission service in order with r's procedure, headers,
// and peer, applying each answer to r before asking the next, so a later
// service sees the mutations of an earlier one. The error is the refusal: the
// connect error the service returned, or Unavailable when it could not be
// reached, and r is not to be forwarded.
func (c *Chain) Admit(ctx context.Context, r *http.Request) error {
	for _, spec := range c.steps {
		if err := c.admit(ctx, spec, r); err != nil {
			return err
		}
	}

	return nil
}

// admit asks one admission service, under an `admit` span naming the
// service's procedure, and applies its answer to r.
func (c *Chain) admit(ctx context.Context, spec connect.Spec, r *http.Request) error {
	ctxSpan := trace.SpanFromContext(ctx)
	tracer := ctxSpan.TracerProvider().Tracer(tracerName)
	ctx, span := tracer.Start(ctx, "admit")
	defer span.End()

	connectotel.Name(span, spec.Procedure)

	var response admissionpb.AdmissionResponse

	if err := c.caller.CallUnary(ctx, spec, request(r), &response); err != nil {
		return c.refuse(ctx, spec.Procedure, err)
	}

	apply(r, &response)

	return nil
}

// refuse turns err into the refusal answered to the client, and records it
// on the admission call's span.
func (c *Chain) refuse(ctx context.Context, procedure string, err error) error {
	if _, ok := errors.AsType[*connect.Error](err); !ok {
		// The service was not reached, or answered outside the protocol;
		// either way nothing admitted the request. What it answered with, if
		// anything, stays local.
		err = connect.NewError(
			connect.CodeUnavailable,
			"admission service unavailable",
		).WithCause(err)
	}

	code := connect.CodeOf(err)

	logger.FromContext(ctx).InfoContext(ctx, "Admission refused",
		slog.String("procedure", procedure),
		slog.String("code", code.String()),
		slog.Any("error", err))

	connectotel.Fail(trace.SpanFromContext(ctx), code)

	return err
}

// request describes r as it is right now: the procedure it names, every
// header, and the connection it arrived on.
func request(r *http.Request) *admissionpb.AdmissionRequest {
	headers := make(map[string]*admissionpb.Values, len(r.Header))
	for name, values := range r.Header {
		headers[name] = &admissionpb.Values{Values: slices.Clone(values)}
	}

	peer := &admissionpb.Peer{Address: r.RemoteAddr}
	if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
		peer.Certificate = r.TLS.PeerCertificates[0].Raw
	}

	return &admissionpb.AdmissionRequest{
		Procedure: r.URL.Path,
		Headers:   headers,
		Peer:      peer,
	}
}

// apply changes r the way response says: removals, then sets, each replacing
// every value of its name, then the procedure when one is named.
func apply(r *http.Request, response *admissionpb.AdmissionResponse) {
	for _, name := range response.GetRemove() {
		r.Header.Del(name)
	}

	for _, header := range response.GetSet() {
		r.Header.Del(header.GetName())

		for _, value := range header.GetValues() {
			r.Header.Add(header.GetName(), value)
		}
	}

	if procedure := response.GetProcedure(); procedure != "" {
		r.URL.Path = procedure
		r.URL.RawPath = ""
	}
}
