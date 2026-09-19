package proxy

import (
	"net/http"

	connectcors "connectrpc.com/cors"
	"github.com/rs/cors"

	server "github.com/pbrpc/http-server"
)

// CORSConfiguration is the cross-origin policy's: the browser origins allowed
// to call the gateway, and the request headers a browser may send beyond the
// protocols' own. Both are comma-separated with no surrounding whitespace. No
// origins is no cross-origin handling.
type CORSConfiguration struct {
	AllowedOrigins []string `env:"CORS_ALLOWED_ORIGINS"`
	AllowedHeaders []string `env:"CORS_ALLOWED_HEADERS"`
}

// CORS answers preflights and marks responses for a browser frontend served
// from another origin than the gateway. The methods and headers allowed are
// the ones the Connect protocols use, plus the headers configured; the
// origins are the ones configured. It goes on the server as route middleware,
// so every route on the mux, the gateway's own endpoints and the proxy alike,
// is under it.
func CORS(cfg CORSConfiguration) server.Middleware {
	policy := cors.New(cors.Options{
		AllowedOrigins: cfg.AllowedOrigins,
		AllowedMethods: connectcors.AllowedMethods(),
		AllowedHeaders: append(connectcors.AllowedHeaders(), cfg.AllowedHeaders...),
		ExposedHeaders: connectcors.ExposedHeaders(),
	})

	return func(_ string, next http.Handler) http.Handler {
		return policy.Handler(next)
	}
}
