package main

import (
	"log"
	"os"

	"github.com/Jana-Hassan/ws-migrator/worker"
)

func main() {
	id := getEnv("WORKER_ID", "1")
	listenAddr := getEnv("LISTEN_ADDR", ":8081")

	w := worker.NewWorker(id)
	if err := w.Start(listenAddr); err != nil {
		log.Fatalf("worker failed: %v", err)
	}
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}