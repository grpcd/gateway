// Package proxy forwards requests to the replica discovered for the
// procedure they name, once admitted.
package proxy

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httputil"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"git.sonicoriginal.software/logger"

	"github.com/grpcd/connect-client/discover"
)

// Admit consults the admission chain, changing r as it says. The error is
// the refusal, answered to the client in place of a forward.
type Admit func(ctx context.Context, r *http.Request) error

// Handler forwards every request it serves. It never reads a body and never
// learns a message type: the request is copied to the replica as it arrived,
// and the response is streamed back as it comes, so unary and streaming
// calls over gRPC, gRPC-Web, and Connect all take the one path.
type Handler struct {
	admit  Admit
	proxy  *httputil.ReverseProxy
	errors *connecthttp.ErrorWriter
	log    *slog.Logger

	forwardingErrors metric.Int64Counter
}

// New builds a Handler forwarding over transport, which is the discovery
// transport: the outgoing request is addressed by the grpcd scheme with the
// path unchanged, and the transport routes it to the replica held for that
// procedure. admit runs first, before the lookup, so a procedure it rewrites
// is what is looked up.
func New(transport http.RoundTripper, admit Admit, log *slog.Logger, meter metric.Meter) *Handler {
	if log == nil {
		log = logger.NewNullLogger()
	}

	forwardingErrors, err := meter.Int64Counter(
		"gateway.forwarding.errors.total",
		metric.WithDescription("Total number of requests that could not be forwarded"),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		log.Error("Failed to create forwarding errors metric", slog.Any("error", err))
	}

	h := &Handler{
		admit:            admit,
		errors:           connecthttp.NewErrorWriter(),
		log:              log,
		forwardingErrors: forwardingErrors,
	}

	h.proxy = &httputil.ReverseProxy{
		Transport: transport,
		Rewrite: func(pr *httputil.ProxyRequest) {
			// Only the scheme changes. The path is the procedure, the headers
			// and the Host are the client's, and the transport supplies the
			// replica.
			pr.Out.URL.Scheme = discover.Scheme
		},
		// Every write is flushed as it happens, which is what keeps a
		// streaming response moving instead of buffered.
		FlushInterval: -1,
		ErrorHandler:  h.forwardingError,
	}

	return h
}

// ServeHTTP admits r, then forwards it.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := h.admit(r.Context(), r); err != nil {
		h.write(w, r, err)

		return
	}

	h.proxy.ServeHTTP(w, r)
}

// forwardingError answers a request the transport could not carry: a path
// naming no procedure is Unimplemented, anything else is Unavailable.
func (h *Handler) forwardingError(w http.ResponseWriter, r *http.Request, err error) {
	// What the transport said stays local; the client gets the code and a
	// message that names no address.
	code, message := connect.CodeUnavailable, "upstream unavailable"
	if errors.Is(err, discover.ErrNoProcedure) {
		code, message = connect.CodeUnimplemented, "no such procedure"
	}

	h.log.InfoContext(r.Context(), "Forwarding failed",
		slog.String("procedure", r.URL.Path), slog.String("code", code.String()), slog.Any("error", err))

	if h.forwardingErrors != nil {
		h.forwardingErrors.Add(r.Context(), 1, metric.WithAttributes(
			attribute.String("procedure", r.URL.Path),
			attribute.String("code", code.String()),
		))
	}

	h.write(w, r, connect.NewError(code, message).WithCause(err))
}

// write answers err in the framing of the protocol r speaks, so a gRPC,
// gRPC-Web, or Connect client each reads it as an error of its own.
func (h *Handler) write(w http.ResponseWriter, r *http.Request, err error) {
	if writeErr := h.errors.Write(w, r, err); writeErr != nil {
		h.log.WarnContext(r.Context(), "Failed to write error", slog.Any("error", writeErr))
	}
}
