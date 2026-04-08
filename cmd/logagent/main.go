package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/zcop/log-agent/internal/api"
	"github.com/zcop/log-agent/internal/config"
	logmcp "github.com/zcop/log-agent/internal/mcp"
	"github.com/zcop/log-agent/internal/parser"
	"github.com/zcop/log-agent/internal/store"
	"github.com/zcop/log-agent/internal/tailer"
)

func main() {
	configPath := flag.String("config", "", "path to config.json (optional)")
	mcpMode := flag.String("mcp", "", "run MCP server: 'stdio' for VS Code, 'sse' for remote/VPS deployment")
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
	log.Printf("[main] max entries: %d, hot window: %v", cfg.MaxEntries, cfg.HotWindow)

	logParser := parser.New()
	logStore := store.New(cfg.MaxEntries)
	fileIndex := store.NewFileIndex()

	t := tailer.New(
		cfg.LogDirs,
		cfg.LogGlob,
		cfg.GzipGlob,
		cfg.PollInterval,
		cfg.ReadArchived,
		cfg.HotWindow,
		logParser,
		logStore,
		fileIndex,
	)

	t.LoadExisting()
	t.Start()
	defer t.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		log.Printf("[main] shutting down")
		cancel()
	}()

	switch *mcpMode {
	case "stdio":
		log.Printf("[main] starting MCP server (stdio transport)")
		mcpServer := logmcp.NewServer(logStore, fileIndex, t, cfg.MCPToken, cfg.MCPAllowedIPs)
		if err := mcpServer.ServeStdio(ctx); err != nil {
			log.Fatalf("[main] MCP server error: %v", err)
		}

	case "sse":
		addr := fmt.Sprintf(":%d", cfg.MCPPort)
		log.Printf("[main] starting MCP server (SSE transport on %s)", addr)
		if cfg.MCPToken == "" {
			log.Printf("[main] WARNING: MCP_TOKEN not set — MCP endpoint has no authentication")
		}
		mcpServer := logmcp.NewServer(logStore, fileIndex, t, cfg.MCPToken, cfg.MCPAllowedIPs)
		if err := mcpServer.ServeSSE(ctx, addr); err != nil {
			log.Fatalf("[main] MCP SSE server error: %v", err)
		}

	default:
		// HTTP API server mode (default)
		httpServer := api.NewServer(cfg, logStore, fileIndex)
		go func() {
			if err := httpServer.ListenAndServe(); err != nil {
				log.Fatalf("[main] HTTP server error: %v", err)
			}
		}()

		// Also start MCP SSE if token is configured
		if cfg.MCPToken != "" {
			addr := fmt.Sprintf(":%d", cfg.MCPPort)
			log.Printf("[main] also starting MCP SSE server on %s", addr)
			mcpServer := logmcp.NewServer(logStore, fileIndex, t, cfg.MCPToken, cfg.MCPAllowedIPs)
			go func() {
				if err := mcpServer.ServeSSE(ctx, addr); err != nil {
					log.Printf("[main] MCP SSE server error: %v", err)
				}
			}()
		}

		<-ctx.Done()
	}
}
