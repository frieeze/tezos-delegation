package main

import (
	"context"
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/hlog"
	"github.com/rs/zerolog/log"
)

func main() {
	log := zerolog.New(os.Stderr).With().Timestamp().Logger()
	config, err := loadConfig()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load config")
	}
	if config.Dev {
		log = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	if config.Debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
		log.Info().Interface("config", config).Msg("config loaded")
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
	ctx := log.WithContext(context.Background())

}
