// Package admission consults the admission services a request passes through
// before it is forwarded.
package admission

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strings"

	"connectrpc.com/connect/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"git.sonicoriginal.software/logger"

	admissionpb "github.com/grpcd/protos/admission"
)

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
	log    *slog.Logger

	refusals metric.Int64Counter
}

// New builds the chain for procedures, in the order given, called over
// caller. An empty list admits everything.
func New(procedures []string, caller Caller, log *slog.Logger, meter metric.Meter) *Chain {
	if log == nil {
		log = logger.NewNullLogger()
	}

	steps := make([]connect.Spec, 0, len(procedures))
	for _, procedure := range procedures {
		steps = append(steps, connect.Spec{StreamType: connect.StreamTypeUnary, Procedure: procedure})
	}

	refusals, err := meter.Int64Counter(
		"gateway.admission.refusals.total",
		metric.WithDescription("Total number of requests refused by an admission service"),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		log.Error("Failed to create admission refusals metric", slog.Any("error", err))
	}

	return &Chain{steps: steps, caller: caller, log: log, refusals: refusals}
}

// Parse reads the procedure list GATEWAY_ADMISSION holds: comma-separated,
// in order, blanks ignored.
func Parse(value string) []string {
	var procedures []string

	for part := range strings.SplitSeq(value, ",") {
		if procedure := strings.TrimSpace(part); procedure != "" {
			procedures = append(procedures, procedure)
		}
	}

	return procedures
}

// Admit consults each admission service in order with r's procedure, headers,
// and peer, applying each answer to r before asking the next, so a later
// service sees the mutations of an earlier one. The error is the refusal: the
// connect error the service returned, or Unavailable when it could not be
// reached, and r is not to be forwarded.
func (c *Chain) Admit(ctx context.Context, r *http.Request) error {
	for _, spec := range c.steps {
		var response admissionpb.AdmissionResponse

		if err := c.caller.CallUnary(ctx, spec, request(r), &response); err != nil {
			return c.refuse(ctx, spec.Procedure, err)
		}

		apply(r, &response)
	}

	return nil
}

// refuse turns err into the refusal answered to the client, and counts it.
func (c *Chain) refuse(ctx context.Context, procedure string, err error) error {
	var connectErr *connect.Error
	if !errors.As(err, &connectErr) {
		// The service was not reached, or answered outside the protocol;
		// either way nothing admitted the request. What it answered with, if
		// anything, stays local.
		err = connect.NewError(connect.CodeUnavailable, "admission service unavailable").WithCause(err)
	}

	code := connect.CodeOf(err)

	c.log.InfoContext(ctx, "Admission refused",
		slog.String("procedure", procedure), slog.String("code", code.String()), slog.Any("error", err))

	if c.refusals != nil {
		c.refusals.Add(ctx, 1, metric.WithAttributes(
			attribute.String("procedure", procedure),
			attribute.String("code", code.String()),
		))
	}

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
