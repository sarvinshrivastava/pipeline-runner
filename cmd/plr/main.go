// Command plr is the pipeline-runner time service.
//
//	plr serve        run the service (default)
//	plr healthcheck  exit 0 when /healthz answers 200 (used by the container healthcheck)
//	plr version      print the build version
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
	_ "time/tzdata" // IANA zones must not depend on the image

	"github.com/sarvinshrivastava/pipeline-runner/internal/api"
	"github.com/sarvinshrivastava/pipeline-runner/internal/config"
	"github.com/sarvinshrivastava/pipeline-runner/internal/engine"
	"github.com/sarvinshrivastava/pipeline-runner/internal/metrics"
	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/notify"
	"github.com/sarvinshrivastava/pipeline-runner/internal/secure"
	"github.com/sarvinshrivastava/pipeline-runner/internal/store"
	"github.com/sarvinshrivastava/pipeline-runner/internal/target"
	"github.com/sarvinshrivastava/pipeline-runner/internal/target/httpt"
	"github.com/sarvinshrivastava/pipeline-runner/internal/vault"
	"github.com/sarvinshrivastava/pipeline-runner/internal/web"
)

// version is stamped at build time with -ldflags "-X main.version=<sha>".
var version = "dev"

func main() {
	cmd := "serve"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}
	switch cmd {
	case "version":
		fmt.Println(version)
	case "healthcheck":
		os.Exit(healthcheck())
	case "serve":
		log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
		cfg, err := config.Load()
		if err != nil {
			log.Error("invalid configuration", "err", err)
			os.Exit(1)
		}
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
		defer stop()
		if err := run(ctx, cfg, log, nil); err != nil {
			log.Error("exit", "err", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "usage: plr [serve|healthcheck|version]")
		os.Exit(2)
	}
}

func healthcheck() int {
	port := 3000
	if v := os.Getenv("PORT"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil || p <= 0 || p > 65535 {
			return 1
		}
		port = p
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:"+strconv.Itoa(port)+"/healthz", nil) //nolint:gosec // fixed loopback host, integer port
	if err != nil {
		return 1
	}
	resp, err := http.DefaultClient.Do(req) //nolint:gosec // fixed loopback host, integer port
	if err != nil {
		return 1
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

// ready is told the bound API and metrics addresses (tests listen on :0).
type ready func(apiAddr, metricsAddr string)

// run serves until ctx ends, then shuts down in order: stop accepting, drain
// in-flight HTTP runs for up to 20 s, exit. Runs still marked running are
// recovered at the next boot.
func run(ctx context.Context, cfg config.Config, log *slog.Logger, onReady ready) error {
	if err := os.MkdirAll(filepath.Join(cfg.DataDir, "v2", "logs"), 0o700); err != nil {
		return err
	}
	st, err := store.Open(ctx, filepath.Join(cfg.DataDir, "v2", "plr.db"))
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer func() { _ = st.Close() }()

	vc := vault.New(cfg.SecretManagerURL, cfg.SecretManagerToken)
	pager := notify.New(cfg.NtfyBase, cfg.NtfyToken, cfg.NtfyTopicOK, cfg.NtfyTopicFail, cfg.DashboardURL, log)
	nctx, stopNotify := context.WithCancel(context.Background())
	defer stopNotify()
	go pager.Run(nctx)

	var eng *engine.Engine
	m := metrics.New(func() float64 {
		if eng == nil {
			return 0
		}
		return eng.LoopAge().Seconds()
	})
	executors := map[string]target.Executor{
		model.TargetHTTP: httpt.New(secure.NewHTTPClient(10*time.Second, secure.IPAllowed)),
	}
	eng = engine.New(engine.Deps{
		Store: st, Executors: executors, Secrets: vc, Pager: pager, Metrics: m, Log: log,
		RunTokenKey: cfg.RunTokenKey, EnvKey: cfg.EncryptionKey, MaxHTTPInflight: cfg.MaxHTTPInflight,
		MaxContainers: cfg.MaxContainers, DataDir: cfg.DataDir, RetentionDays: cfg.RunRetentionDays,
		ContainersEnabled: false, // container/inline targets arrive in delivery 1b
	})
	if err := eng.Start(ctx); err != nil {
		return fmt.Errorf("start engine: %w", err)
	}

	apiSrv := &http.Server{
		Handler: api.New(api.Deps{
			Engine: eng, Store: st, Secrets: vc, Log: log, APIToken: cfg.APIToken, RunTokenKey: cfg.RunTokenKey,
			TrustedProxies: cfg.TrustedProxies, Version: version, Static: web.Handler(),
		}),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 70 * time.Second, IdleTimeout: 120 * time.Second,
	}
	metricsSrv := &http.Server{Handler: m.Handler(), ReadHeaderTimeout: 5 * time.Second}

	// Inside the container the API binds all interfaces; compose publishes it
	// on 127.0.0.1 only. /metrics has its own unpublished listener.
	var lc net.ListenConfig
	apiLn, err := lc.Listen(ctx, "tcp", ":"+strconv.Itoa(cfg.Port))
	if err != nil {
		return err
	}
	metricsLn, err := lc.Listen(ctx, "tcp", ":"+strconv.Itoa(cfg.MetricsPort))
	if err != nil {
		_ = apiLn.Close()
		return err
	}
	errs := make(chan error, 2)
	go func() { errs <- serve(apiSrv, apiLn) }()
	go func() { errs <- serve(metricsSrv, metricsLn) }()
	go heartbeat(ctx, cfg.HeartbeatURL, eng, log)
	log.Info("pipeline-runner started", "version", version, "api", apiLn.Addr().String(), "metrics", metricsLn.Addr().String())
	if onReady != nil {
		onReady(apiLn.Addr().String(), metricsLn.Addr().String())
	}

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-errs:
	}
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		eng.Stop(shutdownCtx, time.Now().Add(20*time.Second))
		close(done)
	}()
	_ = apiSrv.Shutdown(shutdownCtx)
	_ = metricsSrv.Shutdown(shutdownCtx)
	log.Info("api closed")
	<-done
	log.Info("engine stopped")
	stopNotify()
	pager.Drain(context.Background())
	return runErr
}

func serve(srv *http.Server, ln net.Listener) error {
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// heartbeat pings an Uptime Kuma push monitor every 60 s while the engine is
// healthy, so the runner's own death is visible from outside.
func heartbeat(ctx context.Context, url string, eng *engine.Engine, log *slog.Logger) {
	if url == "" {
		return
	}
	client := &http.Client{Timeout: 10 * time.Second}
	tick := time.NewTicker(60 * time.Second)
	defer tick.Stop()
	for {
		if eng.Healthy() {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err == nil {
				if resp, err := client.Do(req); err != nil {
					log.Warn("heartbeat failed", "err", err)
				} else {
					_ = resp.Body.Close()
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
