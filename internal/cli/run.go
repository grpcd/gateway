// Package cli assembles and runs the gateway.
package cli

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/caarlos0/env/v11"
	"go.opentelemetry.io/otel"

	connectclient "github.com/pbrpc/connect-client"
	connectserver "github.com/pbrpc/connect-server"
	server "github.com/pbrpc/connect-server"
	"github.com/pbrpc/connect-service/diagnostics"
	"github.com/pbrpc/connect-service/health"
	"github.com/pbrpc/connect-service/service"
	transport "github.com/pbrpc/http-transport"
	"github.com/pbrpc/lifecycle"
	pbrpcotel "github.com/pbrpc/otel"
	svc "github.com/pbrpc/service"

	grpcdclient "github.com/grpcd/connect-client/client"
	"github.com/grpcd/connect-client/discover"

	"github.com/grpcd/gateway/internal/admission"
	"github.com/grpcd/gateway/internal/proxy"
)

// Environment variable names for the gateway's own configuration.
const (
	// EnvAdmission lists the admission procedures, comma-separated, in the
	// order every request passes through them. Unset means none.
	EnvAdmission = "GATEWAY_ADMISSION"

	// EnvCORSAllowedOrigins lists the browser origins allowed to call the
	// gateway, comma-separated. Unset means no cross-origin handling.
	EnvCORSAllowedOrigins = "CORS_ALLOWED_ORIGINS"

	// EnvCORSAllowedHeaders lists request headers a browser may send beyond
	// the protocols' own, comma-separated; Authorization is the usual one.
	EnvCORSAllowedHeaders = "CORS_ALLOWED_HEADERS"
)

// cleanupTimeout bounds stopping the server and flushing telemetry, together.
const cleanupTimeout = 5 * time.Second

// Run serves until a signal arrives or serving fails, and answers with the
// process exit code.
func Run() int {
	ctx := context.Background()

	serveCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	svcCfg := svc.Configuration{Name: "gateway"}
	err := env.Parse(&svcCfg)
	if err != nil {
		slog.Default().Error("Failed to read configuration", slog.Any("error", err))
		return 1
	}

	stack := lifecycle.Stack{}

	log, flush, err := pbrpcotel.Init(ctx, svcCfg.Name, svcCfg.Version)
	if err != nil {
		log.Error("Failed to initialize telemetry", slog.Any("error", err))
		return 1
	}
	stack.Push(lifecycle.Logged(log, "telemetry", flush))

	opts := []server.Option{}

	// Route middleware runs on every route on the mux, the gateway's own
	// endpoints and the proxy alike, so one policy answers every preflight.
	if origins := admission.Parse(os.Getenv(EnvCORSAllowedOrigins)); len(origins) > 0 {
		admissions := admission.Parse(os.Getenv(EnvCORSAllowedHeaders))
		cors := proxy.CORS(origins, admissions)
		mw := server.WithRouteMiddleware(cors)
		opts = append(opts, mw)
	}

	host, err := connectserver.FromEnv(log, opts...)
	if err != nil {
		log.Error("Failed to create connect server", slog.Any("error", err))
		return 1
	}
	stack.Push(lifecycle.Logged(log, "server", host.HTTPHost.Server.Shutdown))

	defer lifecycle.HandleGracefulShutdown(ctx, log, &stack, cleanupTimeout)

	// A gateway routes nothing without discovery, so the address is required.
	configured, err := env.ParseAsWithOptions[grpcdclient.Configuration](env.Options{RequiredIfNoDef: true})
	if err != nil {
		log.Error("Could not read configuration", slog.Any("error", err))
		return 1
	}

	base, err := transport.From(nil)
	if err != nil {
		log.Error("Could not build transport", slog.Any("error", err))
		return 1
	}

	conn := grpcdclient.Connect(configured.GRPCDAddress, base)

	// One discovery per process, over the instrumented transport, so the
	// client span of every request it carries, forwarded ones included, opens
	// once the replica is known and names it. The admission services are the
	// gateway's own dependencies, reached through the holding transport:
	// resolved and watched from startup, every call to one going to the
	// replica held for it. The forwarded requests go through the discovery
	// itself, resolved one by one so each lands where grpcd sends it.
	discovery := discover.New(serveCtx, log, conn, pbrpcotel.NewTransport(base))
	httpClient := &http.Client{Transport: discovery.Held()}

	meter := otel.Meter(svcCfg.Name)

	procedures := admission.Parse(os.Getenv(EnvAdmission))
	cnClient := connectclient.New(httpClient, discover.BaseURL, nil)
	chain := admission.New(procedures, cnClient, log, meter)

	// grpcd is asked its own health over the connection, and each admission
	// service is reported from the replica its procedure is on.
	checks := diagnostics.Checks{grpcdclient.CheckName: grpcdclient.Check(conn)}

	for _, procedure := range procedures {
		var upstream *discover.Upstream
		upstream, err = discovery.Upstream(discover.URL(procedure))
		if err != nil {
			log.Error("Admission procedure is not a procedure",
				slog.String("procedure", procedure), slog.Any("error", err))
			return 1
		}

		checks[procedure] = diagnostics.NewUpstreamCheck(httpClient, upstream)

		// The wait for a grpcd that cannot be reached, or an admission
		// service nothing has registered yet, is not the listener's to bear:
		// a resolution that fails here is logged, and the first admission
		// call resolves again.
		go func() {
			if err = upstream.Resolve(serveCtx); err != nil {
				log.Warn("Admission service not resolved at startup",
					slog.String("procedure", procedure), slog.Any("error", err))
			}
		}()
	}

	if _, err = service.Register(
		host.Server,
		host.HTTPHost.Mux,
		health.NewServer(),
		checks,
		nil,
	); err != nil {
		log.Error("Failed to register services", slog.Any("error", err))
		return 1
	}

	// The gateway's own routes are longer patterns than "/", so the mux gives
	// them precedence and everything else is forwarded.
	host.HTTPHost.Mux.Handle("/", proxy.New(discovery, chain.Admit, log, meter))

	lis, err := net.Listen("tcp", svcCfg.Address)
	if err != nil {
		log.Error("Failed to create listener", slog.Any("error", err))
		return 1
	}

	log = log.With(slog.String("address", lis.Addr().String()))

	serveErr := make(chan error, 1)
	go func() { serveErr <- host.Serve(lis) }()

	log.Info("Gateway listening", slog.Any("admission", procedures))

	select {
	case err := <-serveErr:
		if err != nil {
			log.Error("Failed to serve", slog.Any("error", err))
			return 1
		}
	case <-serveCtx.Done():
	}

	return 0
}
