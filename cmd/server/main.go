// Command db3 runs the distributed device serial-number HTTP service.
//
// It wires the IdGen core (a snowflake derivative that emits 16-digit
// serial numbers) to a MySQL-backed base-value store and exposes it through
// a Gin HTTP server on /api/v1/next-id.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/kl/db3/internal/api"
	"github.com/kl/db3/internal/config"
	"github.com/kl/db3/internal/idgen"
	"github.com/kl/db3/internal/storage"
)

func main() {
	cfgPath := flag.String("config", "configs/config.yaml", "path to the configuration file")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	gin.SetMode(cfg.Server.Mode)

	// Storage / persistence bootstrap with a bounded init timeout.
	initCtx, initCancel := context.WithTimeout(context.Background(), 15*time.Second)
	store, err := storage.NewMySQLStore(initCtx, cfg.MySQL)
	initCancel()
	if err != nil {
		log.Fatalf("init storage: %v", err)
	}
	defer func() {
		if cerr := store.Close(); cerr != nil {
			log.Printf("close storage: %v", cerr)
		}
	}()

	// Core algorithm library + its persistence-aware manager.
	mgr := idgen.NewGeneratorManager(store, cfg.IDGen.EpochTime(), cfg.IDGen.MaxClockBackwardMs)

	handler := api.NewHandler(mgr)
	router := api.NewRouter(handler)

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Server.Port),
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("db3 id service listening on %s (mode=%s)", srv.Addr, cfg.Server.Mode)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	// Graceful shutdown on interrupt / termination.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("shutdown signal received, draining...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("forced shutdown: %v", err)
	}
	log.Println("server exited cleanly")
}
