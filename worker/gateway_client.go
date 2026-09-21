package worker

import (
	"context"
	"time"

	migratorpb "github.com/Jana-Hassan/ws-migrator/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type gatewayClient struct {
	conn   *grpc.ClientConn
	client migratorpb.GatewayServiceClient
}

func newGatewayClient(addr string) (*gatewayClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &gatewayClient{
		conn:   conn,
		client: migratorpb.NewGatewayServiceClient(conn),
	}, nil
}

func (c *gatewayClient) drain(workerID, reason, token string) (*migratorpb.DrainResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return c.client.Drain(ctx, &migratorpb.DrainRequest{
		WorkerId: workerID,
		Reason:   reason,
		Token:    token,
	})
}

func (c *gatewayClient) register(workerID, wsAddr, grpcAddr, token string) (*migratorpb.RegisterResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return c.client.Register(ctx, &migratorpb.RegisterRequest{
		WorkerId: workerID,
		WsAddr:   wsAddr,
		GrpcAddr: grpcAddr,
		Token:    token,
	})
}

func (c *gatewayClient) unregister(workerID, token string) (*migratorpb.UnregisterResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return c.client.Unregister(ctx, &migratorpb.UnregisterRequest{
		WorkerId: workerID,
		Token:    token,
	})
}

func (c *gatewayClient) close() {
	c.conn.Close()
}
