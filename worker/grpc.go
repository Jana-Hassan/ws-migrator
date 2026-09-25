package worker

import (
	"context"
	"log"
	"time"

	migratorpb "github.com/Jana-Hassan/ws-migrator/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type grpcServer struct {
	migratorpb.UnimplementedWorkerServiceServer
	sessions       *sessionRegistry
	workerID       string
	onCreate       func(sessionID string) State
	healthInterval time.Duration
}

func newGRPCServer(workerID string, sessions *sessionRegistry, onCreate func(sessionID string) State, healthInterval time.Duration) *grpcServer {
	return &grpcServer{
		workerID:       workerID,
		sessions:       sessions,
		onCreate:       onCreate,
		healthInterval: healthInterval,
	}
}

func (g *grpcServer) Restore(ctx context.Context, req *migratorpb.RestoreRequest) (*migratorpb.RestoreResponse, error) {
	if len(req.State) == 0 {
		if req.AppliedSeq > 0 && g.onCreate != nil {
			g.sessions.stage(req.SessionId, g.onCreate(req.SessionId), req.AppliedSeq)
		}
		return &migratorpb.RestoreResponse{Ok: true}, nil
	}
	if g.onCreate == nil {
		return &migratorpb.RestoreResponse{Ok: false, Error: "worker has no session state factory"}, nil
	}

	state := g.onCreate(req.SessionId)
	if err := state.Restore(req.State); err != nil {
		return &migratorpb.RestoreResponse{Ok: false, Error: err.Error()}, nil
	}

	g.sessions.stage(req.SessionId, state, req.AppliedSeq)
	log.Printf("[worker-%s] restored session %s", g.workerID, req.SessionId)

	return &migratorpb.RestoreResponse{Ok: true}, nil
}

func (g *grpcServer) Checkpoint(ctx context.Context, req *migratorpb.CheckpointRequest) (*migratorpb.CheckpointResponse, error) {
	session, ok := g.sessions.get(req.SessionId)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "session %s not found", req.SessionId)
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	var stateBytes []byte
	if session.state != nil {
		var err error
		stateBytes, err = session.state.Serialize()
		if err != nil {
			return &migratorpb.CheckpointResponse{
				SessionId: req.SessionId,
				Ok:        false,
				Error:     err.Error(),
			}, nil
		}
	}

	log.Printf("[worker-%s] checkpointed session %s", g.workerID, req.SessionId)

	return &migratorpb.CheckpointResponse{
		SessionId:  req.SessionId,
		State:      stateBytes,
		Ok:         true,
		AppliedSeq: session.appliedSeq,
	}, nil
}

func (g *grpcServer) HealthStream(req *migratorpb.HealthStreamRequest, stream grpc.ServerStreamingServer[migratorpb.HealthUpdate]) error {
	ticker := time.NewTicker(g.healthInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-ticker.C:
			err := stream.Send(&migratorpb.HealthUpdate{
				WorkerId:     g.workerID,
				SessionCount: int32(g.sessions.count()),
				Status:       migratorpb.WorkerStatus_WORKER_STATUS_SERVING,
				UnixMs:       time.Now().UnixMilli(),
			})
			if err != nil {
				return err
			}
		}
	}
}
