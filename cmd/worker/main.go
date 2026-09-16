package main

import (
	"log"
	"os"

	"github.com/Jana-Hassan/ws-migrator/worker"
)

func main() {
		cfg := worker.Config{
		ID:       getEnv("WORKER_ID", "1"),
		WSAddr:   getEnv("LISTEN_ADDR", ":8081"),
		GRPCAddr: getEnv("GRPC_ADDR", ":9091"),
	}

	w := worker.NewWorker(cfg)
	if err := w.Start(cfg);
	 err != nil {
		log.Fatalf("worker failed: %v", err)
	}
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}