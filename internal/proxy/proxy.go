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
	"go.opentelemetry.io/otel/trace"

	"git.sonicoriginal.software/logger"

	"github.com/grpcd/connect-client/discover"
	connectotel "github.com/pbrpc/connect-otel"
)

// tracerName is this package's instrumentation scope.
const tracerName = "grpcd/gateway/internal/proxy"

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
}

// New builds a Handler forwarding over transport, which is the discovery
// transport: the outgoing request is addressed by the grpcd scheme with the
// path unchanged, and the transport routes it to the replica held for that
// procedure. admit runs first, before the lookup, so a procedure it rewrites
// is what is looked up.
func New(transport http.RoundTripper, admit Admit) *Handler {
	h := &Handler{
		admit:  admit,
		errors: connecthttp.NewErrorWriter(),
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

// ServeHTTP names the RPC the client asked for on the request's span, admits
// r, then forwards it under a `forward` span named by the procedure as
// admission left it, which may differ.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctxSpan := trace.SpanFromContext(ctx)
	log := logger.FromContext(ctx)

	connectotel.Name(ctxSpan, r.URL.Path)

	log.DebugContext(ctx, "Admitting", slog.String("procedure", r.URL.Path))

	if err := h.admit(ctx, r); err != nil {
		h.write(w, r, err)

		return
	}

	log.DebugContext(ctx, "Admitted", slog.String("procedure", r.URL.Path))

	tracer := ctxSpan.TracerProvider().Tracer(tracerName)
	ctx, span := tracer.Start(ctx, "forward")
	defer span.End()

	connectotel.Name(span, r.URL.Path)

	log.DebugContext(ctx, "Forwarding", slog.String("procedure", r.URL.Path))

	// Returns once the whole response, streaming included, has been relayed
	// to the client, or the client has gone.
	h.proxy.ServeHTTP(w, r.WithContext(ctx))

	log.DebugContext(ctx, "Forwarded", slog.String("procedure", r.URL.Path))
}

// forwardingError answers a request the transport could not carry: a path
// naming no procedure is Unimplemented, anything else is Unavailable. r is
// the outgoing request, whose context is the forward's.
func (h *Handler) forwardingError(w http.ResponseWriter, r *http.Request, err error) {
	// What the transport said stays local; the client gets the code and a
	// message that names no address.
	code, message := connect.CodeUnavailable, "upstream unavailable"
	if errors.Is(err, discover.ErrNoProcedure) {
		code, message = connect.CodeUnimplemented, "no such procedure"
	}

	ctx := r.Context()

	logger.FromContext(ctx).InfoContext(ctx, "Forwarding failed",
		slog.String("procedure", r.URL.Path),
		slog.String("code", code.String()),
		slog.Any("error", err))

	h.write(w, r, connect.NewError(code, message).WithCause(err))
}

// write records err on the span of the request it answers and answers it in
// the framing of the protocol r speaks, so a gRPC, gRPC-Web, or Connect client
// each reads it as an error of its own.
func (h *Handler) write(w http.ResponseWriter, r *http.Request, err error) {
	ctx := r.Context()

	connectotel.Fail(trace.SpanFromContext(ctx), connect.CodeOf(err))

	if writeErr := h.errors.Write(w, r, err); writeErr != nil {
		logger.FromContext(ctx).WarnContext(ctx, "Failed to write error",
			slog.Any("error", writeErr))
	}
}
