package main

import (
	"log"
	"os"

	"github.com/Jana-Hassan/ws-migrator/gateway"
)

func main() {
	workerAddr := getEnv("WORKER_ADDR", "ws://localhost:8081/ws")
	listenAddr := getEnv("LISTEN_ADDR", ":8080")

	server := gateway.NewServer(workerAddr)
	if err := server.Start(listenAddr); err != nil {
		log.Fatalf("gateway failed: %v", err)
	}
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}