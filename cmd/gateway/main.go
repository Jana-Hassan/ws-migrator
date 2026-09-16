package main

import (
	"log"
	
	"context"
	"os"
	"strings"

	"github.com/Jana-Hassan/ws-migrator/gateway"
	"github.com/Jana-Hassan/ws-migrator/store"
)

func main() {

	listenAddr := getEnv("LISTEN_ADDR", ":8080")
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	workersRaw := getEnv("WORKERS", "worker-1|ws://localhost:8081/ws|localhost:9091")

	s := store.New(redisAddr)

	if err := s.Ping(context.Background()); err != nil {
		log.Fatalf("cannot reach Redis at %s: %v", redisAddr, err)
	}
	log.Printf("connected to Redis at %s", redisAddr)

	pool := gateway.NewPool()
	for _, w := range parseWorkers(workersRaw) {
		pool.Add(w)
		log.Printf("registered worker %s at %s (grpc %s)", w.ID, w.WSAddr, w.GRPCAddr)
	}

	if pool.Count() == 0 {
		log.Fatalf("no workers configured")
	}

	server := gateway.NewServer(pool, s)
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

func parseWorkers(raw string) []*gateway.WorkerInfo {
	var workers []*gateway.WorkerInfo
	for _, entry := range strings.Split(raw, ",") {
		parts := strings.Split(entry, "|")
		if len(parts) != 3 {
			continue
		}
		workers = append(workers, &gateway.WorkerInfo{
			ID:       parts[0],
			WSAddr:   parts[1],
			GRPCAddr: parts[2],
		})
	}
	return workers
}