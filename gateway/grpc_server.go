package gateway

import (
	"context"
	"log"
	"net"

	migratorpb "github.com/Jana-Hassan/ws-migrator/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func (s *Server) SetRegisterToken(token string) {
	s.registerToken = token
}

func (s *Server) authorize(token string) error {
	if s.registerToken == "" || token == s.registerToken {
		return nil
	}
	return status.Error(codes.Unauthenticated, "invalid token")
}

func (g *grpcServer) Register(ctx context.Context, req *migratorpb.RegisterRequest) (*migratorpb.RegisterResponse, error) {
	if err := g.server.authorize(req.Token); err != nil {
		return nil, err
	}
	if req.WorkerId == "" || req.WsAddr == "" || req.GrpcAddr == "" {
		return &migratorpb.RegisterResponse{Ok: false, Error: "worker_id, ws_addr and grpc_addr are required"}, nil
	}

	changed := g.server.pool.Update(&WorkerInfo{
		ID:       req.WorkerId,
		WSAddr:   req.WsAddr,
		GRPCAddr: req.GrpcAddr,
	})

	if changed {
		g.server.grpcClient.Invalidate(req.WorkerId)
		log.Printf("worker %s registered at %s (grpc %s)", req.WorkerId, req.WsAddr, req.GrpcAddr)
	}

	return &migratorpb.RegisterResponse{Ok: true}, nil
}

func (g *grpcServer) Unregister(ctx context.Context, req *migratorpb.UnregisterRequest) (*migratorpb.UnregisterResponse, error) {
	if err := g.server.authorize(req.Token); err != nil {
		return nil, err
	}

	g.server.pool.Remove(req.WorkerId)
	g.server.grpcClient.Invalidate(req.WorkerId)
	log.Printf("worker %s unregistered", req.WorkerId)

	return &migratorpb.UnregisterResponse{Ok: true}, nil
}

func (g *grpcServer) Drain(ctx context.Context, req *migratorpb.DrainRequest) (*migratorpb.DrainResponse, error) {
	if err := g.server.authorize(req.Token); err != nil {
		return nil, err
	}

	g.server.pool.SetDraining(req.WorkerId, true)

	sessions := g.server.registry.AllOnWorker(req.WorkerId)
	log.Printf("drain requested for %s (%s): %d sessions", req.WorkerId, req.Reason, len(sessions))

	targets := g.server.newLoadTracker(req.WorkerId)

	accepted := true
	for _, session := range sessions {
		target, ok := targets.next()
		if !ok {
			log.Printf("drain: no target available for session %s", session.ID)
			accepted = false
			break
		}

		select {
		case session.migrateChannel <- migrateRequest{target: target.ID, fallback: true}:
		default:
		}
	}

	return &migratorpb.DrainResponse{
		Accepted:     accepted,
		SessionCount: int32(len(sessions)),
	}, nil
}
