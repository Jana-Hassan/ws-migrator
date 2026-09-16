package main

import (
	"log"
	"os"
	"context"

	"github.com/Jana-Hassan/ws-migrator/gateway"
	"github.com/Jana-Hassan/ws-migrator/store"
)

func main() {
	workerAddr := getEnv("WORKER_ADDR", "ws://localhost:8081/ws")
	listenAddr := getEnv("LISTEN_ADDR", ":8080")
	redisAddr  := getEnv("REDIS_ADDR", "localhost:6379")

	s := store.New(redisAddr)

	if err := s.Ping(context.Background()); err != nil {
		log.Fatalf("cannot reach Redis at %s: %v", redisAddr, err)
	}
	log.Printf("connected to Redis at %s", redisAddr)


	server := gateway.NewServer(workerAddr, s)
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