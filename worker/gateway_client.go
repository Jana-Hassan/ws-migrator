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

func (c *gatewayClient) drain(workerID, reason string) (*migratorpb.DrainResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return c.client.Drain(ctx, &migratorpb.DrainRequest{
		WorkerId: workerID,
		Reason:   reason,
	})
}

func (c *gatewayClient) close() {
	c.conn.Close()
}
