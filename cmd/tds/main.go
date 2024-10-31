package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/frieeze/tezos-delegation/internal/handlers"
	"github.com/frieeze/tezos-delegation/internal/middleware"
	"github.com/frieeze/tezos-delegation/internal/store"
	"github.com/frieeze/tezos-delegation/internal/sync"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/hlog"
)

type config struct {
	dev          bool
	debug        bool
	dbPath       string
	history      bool
	api          string
	syncInterval time.Duration
	port         int
}

func loadConfig() (config, error) {
	dev := flag.Bool("dev", false, "enable development mode")
	debug := flag.Bool("debug", false, "enable debug logging")
	dbPath := flag.String("db", "delegations.db", "path to the database file")
	noHistory := flag.Bool("nohistory", false, "disable history sync")
	api := flag.String("api", "https://api.tzkt.io/v1/operations/delegations", "tzkt api delegation endpoint")
	syncInterval := flag.String("sync", "1m", "sync interval, should be a duration string")
	port := flag.Int("port", 8080, "http server port")

	flag.Parse()

	si, err := time.ParseDuration(*syncInterval)
	if err != nil {
		return config{}, err
	}

	return config{
		dev:          *dev,
		debug:        *debug,
		dbPath:       *dbPath,
		history:      !*noHistory,
		api:          *api,
		syncInterval: si,
		port:         *port,
	}, nil
}

func main() {
	log := zerolog.New(os.Stderr).With().Timestamp().Logger()
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load config")
	}
	if cfg.dev {
		log = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	if cfg.debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
	ctx := log.WithContext(context.Background())

	// ****************APP****************
	log.Info().Msg("create store")
	store, err := store.NewSqLite(ctx, cfg.dbPath)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create store")
	}

	if cfg.history {
		log.Info().Msg("start history sync")
		history := sync.NewHistory(cfg.api, store)
		defer history.Stop()
		go func() {
			err = history.Sync(ctx, "", "")
			if err != nil {
				log.Fatal().Err(err).Msg("failed to sync history")
			}
			log.Info().Msg("history sync done")
		}()
	}

	log.Info().Msg("start live sync")
	syncer := sync.NewLive(cfg.api, cfg.syncInterval, store)
	defer syncer.Stop()

	err = syncer.Sync(ctx, "")
	if err != nil {
		log.Fatal().Err(err).Msg("failed to sync live")
	}

	// ****************HTTP SERVER****************
	log.Info().Int("port", cfg.port).Msg("start http server")
	h := handlers.Handlers{Store: store}
	router := http.NewServeMux()
	router.Handle("/xzt/", http.StripPrefix("/xzt", h.AddXTZRoutes()))

	use := middleware.Use(
		hlog.RequestIDHandler("req_id", "Request-Id"),
		middleware.Logger(),
		hlog.NewHandler(log),
	)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.port),
		Handler: use(router),
	}

	go func() {
		err = server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("http server failed")
		}
	}()

	// ****************GRACEFUL SHUTDOWN****************
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	<-stop

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	go func() {
		<-ctx.Done()
		if ctx.Err() == context.DeadlineExceeded {
			log.Fatal().Msg("graceful shutdown timed out.. forcing exit.")
		}
	}()

	log.Info().Msg("stopping app")
}
