// Command simulator runs the Form3 scheme simulator TCP server.
package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/form3tech-oss/interview-simulator/internal/scheme"
)

func main() {
	cfg := scheme.DefaultConfig()

	srv := scheme.NewServer(cfg)
	if err := srv.Listen(); err != nil {
		log.Fatalf("failed to start scheme simulator: %v", err)
	}
	log.Printf("scheme simulator listening on %s", srv.Addr())

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve() }()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("received %s, shutting down (grace period %s)", sig, cfg.GracePeriod)
		srv.Shutdown()
		log.Print("shutdown complete")
	case err := <-serveErr:
		if err != nil {
			log.Fatalf("scheme simulator stopped unexpectedly: %v", err)
		}
	}
}
