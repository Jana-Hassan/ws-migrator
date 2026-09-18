package gateway

import (
	"context"
	"fmt"
	"sync"
	"time"

	migratorpb "github.com/Jana-Hassan/ws-migrator/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GRPCClient struct {
	mu    sync.RWMutex
	conns map[string]*grpc.ClientConn
	pool  *Pool
}

func NewGRPCClient(pool *Pool) *GRPCClient {
	return &GRPCClient{
		conns: make(map[string]*grpc.ClientConn),
		pool:  pool,
	}
}

func (c *GRPCClient) connect(workerID string) (migratorpb.WorkerServiceClient, error) {
	c.mu.RLock()
	conn, ok := c.conns[workerID] //check if its already connected
	c.mu.RUnlock()

	if ok {
		return migratorpb.NewWorkerServiceClient(conn), nil
	}

	worker, ok := c.pool.Get(workerID)
	if !ok {
		return nil, fmt.Errorf("worker %s not in pool", workerID)
	}

	c.mu.Lock() //not connected so need to create
	defer c.mu.Unlock()

	if conn, ok := c.conns[workerID]; ok {
		return migratorpb.NewWorkerServiceClient(conn), nil
	}

	conn, err := grpc.NewClient(
		worker.GRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("dial worker %s at %s: %w", workerID, worker.GRPCAddr, err)
	}

	c.conns[workerID] = conn
	return migratorpb.NewWorkerServiceClient(conn), nil
}

func (c *GRPCClient) Checkpoint(ctx context.Context, workerID, sessionID string) (*migratorpb.CheckpointResponse, error) {
	client, err := c.connect(workerID)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return client.Checkpoint(ctx, &migratorpb.CheckpointRequest{
		SessionId: sessionID,
	})
}

func (c *GRPCClient) Restore(ctx context.Context, workerID, sessionID string, state []byte) (*migratorpb.RestoreResponse, error) {
	client, err := c.connect(workerID)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return client.Restore(ctx, &migratorpb.RestoreRequest{
		SessionId: sessionID,
		State:     state,
	})
}

func (c *GRPCClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.conns {
		conn.Close()
	}
}
