package main

import (
	"log/slog"
	"os"

	"notifications/app"
	"notifications/config"
)

func main() {
	cfg, err := config.LoadProducer()
	if err != nil {
		slog.Error("load config", "err", err)
		os.Exit(1)
	}

	if err := app.RunProducer(cfg); err != nil {
		slog.Error("service stopped with error", "err", err)
		os.Exit(1)
	}
}
