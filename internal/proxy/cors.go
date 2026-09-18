package proxy

import (
	"net/http"

	connectcors "connectrpc.com/cors"
	"github.com/rs/cors"

	server "github.com/pbrpc/http-server"
)

// CORS answers preflights and marks responses for a browser frontend served
// from another origin than the gateway. The methods and headers allowed are
// the ones the Connect protocols use, plus whatever headers the operator
// names; the origins are the operator's. It goes on the server as route
// middleware, so every route on the mux, the gateway's own endpoints and the
// proxy alike, is under it.
func CORS(origins, headers []string) server.Middleware {
	policy := cors.New(cors.Options{
		AllowedOrigins: origins,
		AllowedMethods: connectcors.AllowedMethods(),
		AllowedHeaders: append(connectcors.AllowedHeaders(), headers...),
		ExposedHeaders: connectcors.ExposedHeaders(),
	})

	return func(_ string, next http.Handler) http.Handler {
		return policy.Handler(next)
	}
}
