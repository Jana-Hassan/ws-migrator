package gateway

import (
	"context"
	"log"
	"net"

	migratorpb "github.com/Jana-Hassan/ws-migrator/proto"
	"google.golang.org/grpc"
)

type grpcServer struct {
	migratorpb.UnimplementedGatewayServiceServer
	server *Server
}

func (s *Server) StartGRPC(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	log.Printf("gateway gRPC starting on %s", addr)
	return s.StartGRPCOn(lis)
}

func (s *Server) StartGRPCOn(lis net.Listener) error {
	srv := grpc.NewServer()
	migratorpb.RegisterGatewayServiceServer(srv, &grpcServer{server: s})
	return srv.Serve(lis)
}

func (g *grpcServer) Drain(ctx context.Context, req *migratorpb.DrainRequest) (*migratorpb.DrainResponse, error) {
	g.server.pool.SetDraining(req.WorkerId, true)

	sessions := g.server.registry.AllOnWorker(req.WorkerId)
	log.Printf("drain requested for %s (%s): %d sessions", req.WorkerId, req.Reason, len(sessions))

	accepted := true
	for _, session := range sessions {
		target, ok := g.server.pickWorker(req.WorkerId)
		if !ok {
			log.Printf("drain: no target available for session %s", session.ID)
			accepted = false
			break
		}

		select {
		case session.migrateChannel <- target.ID:
		default:
		}
	}

	return &migratorpb.DrainResponse{
		Accepted:     accepted,
		SessionCount: int32(len(sessions)),
	}, nil
}
