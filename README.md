# grpcd Connect Gateway

The public entry point for services registered with
[grpcd](https://github.com/grpcd). It forwards every request to a replica of the
service that serves the request's procedure, resolved through grpcd, after
passing it through the admission services it is configured with.

## About

The gateway is a byte proxy. It never decodes a body, never learns a message
type, and holds no per-service or per-method configuration. A request comes in
naming a procedure, `/package.Service/Method`; the gateway consults its
admission services, looks the procedure up through grpcd, and copies the request
to the replica it holds for it, streaming the response back as it comes. Unary
and streaming calls over gRPC, gRPC-Web, and Connect all take that one path.

Nothing about any deployment is compiled in. What the gateway does with a
request beyond forwarding it is decided by admission services reached through
the same discovery, so the same binary serves any set of services behind any
policy.

## Installation

```bash
go install github.com/grpcd/gateway@latest
```

## Request Path

1. The gateway's own endpoints answer directly: `/healthz`, the
   [info](https://github.com/pbrpc/connect-protos) and diagnostics services.
2. Everything else is admitted. Each admission procedure in `GATEWAY_ADMISSION`
   is called in order with the request's procedure, headers, and peer. Its
   answer names headers to remove and set on the forwarded request, and may name
   a different procedure to forward to; the next admission service sees the
   result. An error from any of them ends the request: it is written to the
   client in the client's protocol and nothing is forwarded. An admission
   service that cannot be reached is a refusal too.
3. The procedure the request names after admission is resolved, for this request
   alone: the gateway asks grpcd for it, probes the candidate it is given,
   reports one it cannot reach dead and takes the next, and closes the lookup on
   the one that answers. Nothing is kept between requests, so each lands where
   grpcd sends it and a new replica takes its share of traffic as soon as it
   registers. A procedure nothing serves is answered at once; the gateway never
   waits on grpcd for a registration on a client's behalf.
4. The request is forwarded as it arrived: path, headers, and body untouched
   beyond what admission changed, with only the URL scheme rewritten to
   `grpcd:///`, which is what resolves it. The response streams back with every
   write flushed.

A request whose path does not name a procedure is answered `Unimplemented`; one
that grpcd cannot resolve, or whose replica stops answering between the probe
and the send, is answered `Unavailable`. Nothing is retried: an inbound request
carries no rebuildable body, so a forward that fails is reported, never
replayed.

The admission services are the gateway's own dependencies and are reached the
way a service reaches one: resolved at startup, held, and watched, so every
admission call goes to the replica held for it and grpcd moves the gateway when
a new replica of an admission service registers.

## Admission

The contract is two messages in [grpcd/protos](https://github.com/grpcd/protos),
`AdmissionRequest` and `AdmissionResponse`, with no shared service. An admission
service declares its own unary RPC on them and registers it with grpcd like any
other method:

```proto
service Authenticator {
  rpc Authenticate(admission.AdmissionRequest) returns (admission.AdmissionResponse);
}
```

The gateway is told the procedure name, `/auth.Authenticator/Authenticate`, and
calls it with no generated client: the procedure comes from configuration, the
types from the protos. The gateway holds no header names, no notion of a
credential, and no notion of a principal; those are the admission service's.

An authenticator, for example, answers every request with
`remove: [x-principal-id]`, so a client cannot supply its own, and when a bearer
token is present and verifies, `set: [x-principal-id=<subject>]`; a token that
does not verify is an `Unauthenticated` error, which the client receives and the
backend never sees.

What admission can express is bounded by what the gateway holds before reading a
body: the procedure, the headers, and the peer. The body is never sent and
cannot be changed, the response phase has no hook, and the only early exit is an
error.

## Scaling

Every instance is stateless. It keeps nothing for the procedures it forwards;
its only held state is one replica and one `Watch` per admission procedure,
rebuilt from grpcd on demand. Instances are added behind the TLS terminator or a
load balancer with no coordination. Per request, the cost is one unary call per
admission service over a held connection, one `Discover` round trip and one
probe to resolve the target, and one forward. Backend connections pool by
address in the standard transport, so a replica serving many requests is dialed
once.

## Configuration

- `GRPCD_ADDRESS` — the grpcd service, `host:port`. Required: a gateway routes
  nothing without discovery.
- `GATEWAY_ADMISSION` — admission procedures, comma-separated with no spaces,
  in order. Unset means every request is admitted as it arrived.
- `GRPC_SERVER_ADDRESS` — the listen address, `:50051` by default.
- `TLS_CERT`, `TLS_KEY` — the listener's certificate and key as PEM. Unset means
  the listener is cleartext, for a TLS terminator in front of it.
  `TLS_CLIENT_CA` — a PEM CA; when set, every client must present a certificate
  it signed, which is how the origin is closed to anything but the terminator.
  Each holds the material itself, not a path.
- `CORS_ALLOWED_ORIGINS` — browser origins allowed to call the gateway,
  comma-separated with no spaces, for a frontend served from another origin.
  Unset means no cross-origin handling. `CORS_ALLOWED_HEADERS` — request
  headers a browser may send beyond the Connect protocols' own, comma-separated
  with no spaces; `Authorization` is the usual one.
- `SERVICE_NAME`, `SERVICE_VERSION`, `MAX_CONNECTION_IDLE`,
  `HTTP_SERVER_IDLE_TIMEOUT` `HTTP2_SEND_PING_TIMEOUT`, `HTTP2_PING_TIMEOUT`,
  `OTEL_*`, `LOG_FORMAT` — as every service in the
  [pbrpb ecosystem](https://github.com/pbrpc).

## Observability

Request counts and durations come from the HTTP instrumentation on every route.
The gateway adds `gateway.admission.refusals.total`, by procedure and code, and
`gateway.forwarding.errors.total`, by procedure and code. Its diagnostics report
grpcd by asking grpcd's own health over the connection, and each admission
service from the replica its procedure is on.
