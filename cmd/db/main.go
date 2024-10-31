package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/frieeze/tezos-delegation/internal/store"
	"github.com/frieeze/tezos-delegation/internal/xtz"
	"github.com/rs/zerolog"
)

type config struct {
	debug  bool
	dbPath string
	api    string
	empty  bool
}

func loadConfig() (config, error) {
	debug := flag.Bool("debug", false, "enable debug logging")
	dbPath := flag.String("db", "delegations.db", "path to the database file")
	api := flag.String("api", "https://api.tzkt.io/v1/operations/delegations", "tzkt api delegation endpoint")
	empty := flag.Bool("empty", false, "empty the database")

	flag.Parse()

	return config{
		debug:  *debug,
		dbPath: *dbPath,
		api:    *api,
		empty:  *empty,
	}, nil
}

func main() {
	log := zerolog.New(os.Stderr).With().Timestamp().Logger()
	log = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load config")
	}

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	if cfg.debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
	ctx := log.WithContext(context.Background())

	log.Info().Msg("create store")
	store, err := store.NewSqLite(ctx, cfg.dbPath)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create store")
	}

	// set up stop signal listener
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	if cfg.empty {
		log.Info().Msg("empty store")
		err = store.Empty(ctx)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to empty store")

		}
		log.Info().Msg("done!")
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	log.Info().Msg("start history sync")
	history := xtz.NewHistory(cfg.api, store)
	defer history.Stop()
	go func() {
		err = history.Sync(ctx, "", "")
		if err != nil {
			log.Fatal().Err(err).Msg("failed to sync history")
		}
		log.Info().Msg("history sync done")
		stop <- syscall.SIGINT
	}()

	<-stop

	ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	go func() {
		<-ctx.Done()
		if ctx.Err() == context.DeadlineExceeded {
			log.Fatal().Msg("graceful shutdown timed out.. forcing exit.")
		}
	}()

	log.Info().Msg("stopping app")
}
