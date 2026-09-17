package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/Jana-Hassan/ws-migrator/worker"
)

type Counter struct {
	Count int
}

func (c *Counter) Serialize() ([]byte, error) {
	return []byte(strconv.Itoa(c.Count)), nil
}

func (c *Counter) Restore(data []byte) error {
	n, err := strconv.Atoi(string(data))
	if err != nil {
		return err
	}
	c.Count = n
	return nil
}

func main() {
	cfg := worker.Config{
		ID:       getEnv("WORKER_ID", "1"),
		WSAddr:   getEnv("LISTEN_ADDR", ":8081"),
		GRPCAddr: getEnv("GRPC_ADDR", ":9091"),
	}

	w := worker.NewWorker(cfg)

	w.OnSessionCreate(func(sessionID string) worker.State {
		return &Counter{}
	})

	w.OnMessage(func(sessionID string, state worker.State, msg []byte) []byte {
		counter := state.(*Counter)
		counter.Count++
		return []byte(fmt.Sprintf("worker=%s count=%d msg=%s", cfg.ID, counter.Count, msg))
	})

	if err := w.Start(); err != nil {
		log.Fatalf("worker failed: %v", err)
	}
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
