package main

import (
	"log"
	"os"

	"github.com/Jana-Hassan/ws-migrator/store"
	"github.com/Jana-Hassan/ws-migrator/worker"
)

func main() {
	cfg := worker.Config{
		ID:            getEnv("WORKER_ID", hostname()),
		WSAddr:        getEnv("LISTEN_ADDR", ":8081"),
		GRPCAddr:      getEnv("GRPC_ADDR", ":9091"),
		GatewayAddr:   getEnv("GATEWAY_ADDR", "localhost:9090"),
		AdvertiseWS:   getEnv("WS_ADVERTISE", ""),
		AdvertiseGRPC: getEnv("GRPC_ADVERTISE", ""),
		RegisterToken: getEnv("REGISTER_TOKEN", ""),
		Store:         store.NewRedis(getEnv("REDIS_ADDR", "localhost:6379")),
	}

	w := worker.NewWorker(cfg)
	if err := w.Start(); err != nil {
		log.Fatalf("worker failed: %v", err)
	}
}

func hostname() string {
	name, err := os.Hostname()
	if err != nil {
		return "worker"
	}
	return name
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
