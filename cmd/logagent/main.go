package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/zcop/log-agent/internal/api"
	"github.com/zcop/log-agent/internal/config"
	"github.com/zcop/log-agent/internal/parser"
	"github.com/zcop/log-agent/internal/store"
	"github.com/zcop/log-agent/internal/tailer"
)

func main() {
	configPath := flag.String("config", "", "path to config.json (optional)")
	flag.Parse()

	cfg := config.Default()
	if *configPath != "" {
		if err := cfg.LoadFromFile(*configPath); err != nil {
			log.Fatalf("failed to load config: %v", err)
		}
	}
	cfg.LoadFromEnv()

	log.Printf("[main] ZCOP Log Agent starting")
	log.Printf("[main] watching directories: %v", cfg.LogDirs)
	log.Printf("[main] max entries: %d", cfg.MaxEntries)

	logParser := parser.New()
	logStore := store.New(cfg.MaxEntries)

	t := tailer.New(
		cfg.LogDirs,
		cfg.LogGlob,
		cfg.GzipGlob,
		cfg.PollInterval,
		cfg.ReadArchived,
		logParser,
		logStore,
	)

	t.LoadExisting()
	t.Start()
	defer t.Stop()

	server := api.NewServer(cfg, logStore)
	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.Fatalf("[main] server error: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("[main] shutting down")
}
