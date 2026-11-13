package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"webdav-agg/src/config"
	"webdav-agg/src/logger"
	"webdav-agg/src/server"
)

func main() {
	cfg, err := config.Load(config.DefaultConfigFileName)
	if err != nil {
		logger.Error("Main", "Failed to load config %q: %v", config.DefaultConfigFileName, err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv, err := server.NewServer(cfg, nil)
	if err != nil {
		logger.Error("Main", "Failed to initialize server: %v", err)
		os.Exit(1)
	}

	shutdownDone := make(chan struct{})
	go func() {
		<-ctx.Done()
		logger.Info("Main", "Shutdown signal received, stopping server...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("Main", "Server shutdown error: %v", err)
		}
		close(shutdownDone)
	}()

	if err := srv.StartWithContext(ctx); err != nil {
		logger.Error("Main", "Server exited with error: %v", err)
		os.Exit(1)
	}

	if ctx.Err() != nil {
		<-shutdownDone
	}
}
