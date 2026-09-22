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
	"uuid"

	"github.com/caarlos0/env/v11"

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

	// The instance id names this process on every span, log line, and metric
	// for as long as it runs.
	log, flush, err := pbrpcotel.Init(ctx, svcCfg.Name, svcCfg.Version, uuid.New().String())
	if err != nil {
		log.Error("Failed to initialize telemetry", slog.Any("error", err))
		return 1
	}
	stack.Push(lifecycle.Logged(log, "telemetry", flush))

	opts := []server.Option{}

	corsCfg, err := env.ParseAs[proxy.CORSConfiguration]()
	if err != nil {
		log.Error("Could not read CORS configuration", slog.Any("error", err))
		return 1
	}

	// Route middleware runs on every route on the mux, the gateway's own
	// endpoints and the proxy alike, so one policy answers every preflight.
	if len(corsCfg.AllowedOrigins) > 0 {
		opts = append(opts, server.WithRouteMiddleware(proxy.CORS(corsCfg)))
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

	admissionCfg, err := env.ParseAs[admission.Configuration]()
	if err != nil {
		log.Error("Could not read admission configuration", slog.Any("error", err))
		return 1
	}

	procedures := admissionCfg.Procedures
	cnClient := connectclient.New(httpClient, discover.BaseURL, nil)
	chain := admission.New(procedures, cnClient)

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

		// Held for the life of the process: resolved now, and again whenever
		// the replica held is dropped, with or without a request arriving to
		// ask.
		go upstream.Hold(serveCtx)
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
	host.HTTPHost.Mux.Handle("/", proxy.New(discovery, chain.Admit))

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
