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

type statsSource interface {
	snapshotStats() (failures, written uint64)
}

type grpcServer struct {
	migratorpb.UnimplementedWorkerServiceServer
	sessions       *sessionRegistry
	workerID       string
	onCreate       func(sessionID string) State
	healthInterval time.Duration
	stats          statsSource
}

func newGRPCServer(workerID string, sessions *sessionRegistry, onCreate func(sessionID string) State, healthInterval time.Duration, stats statsSource) *grpcServer {
	return &grpcServer{
		workerID:       workerID,
		sessions:       sessions,
		onCreate:       onCreate,
		healthInterval: healthInterval,
		stats:          stats,
	}
}

func (g *grpcServer) Restore(ctx context.Context, req *migratorpb.RestoreRequest) (*migratorpb.RestoreResponse, error) {
	outbox, err := decodeOutbox(req.Outbox)
	if err != nil {
		return &migratorpb.RestoreResponse{Ok: false, Error: err.Error()}, nil
	}

	var state State
	if g.onCreate != nil {
		state = g.onCreate(req.SessionId)
	}

	if len(req.State) > 0 {
		if state == nil {
			return &migratorpb.RestoreResponse{Ok: false, Error: "worker has no session state factory"}, nil
		}
		if err := state.Restore(req.State); err != nil {
			return &migratorpb.RestoreResponse{Ok: false, Error: err.Error()}, nil
		}
	}

	g.sessions.stage(req.SessionId, state, req.AppliedSeq, outbox)
	log.Printf("[worker-%s] restored session %s at seq %d with %d unacked frames", g.workerID, req.SessionId, req.AppliedSeq, len(outbox))

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

	if req.Freeze {
		session.frozen = true
	}

	log.Printf("[worker-%s] checkpointed session %s (freeze=%v)", g.workerID, req.SessionId, req.Freeze)

	return &migratorpb.CheckpointResponse{
		SessionId:  req.SessionId,
		State:      stateBytes,
		Ok:         true,
		AppliedSeq: session.appliedSeq,
		Outbox:     encodeOutbox(session.outbox),
	}, nil
}

func (g *grpcServer) Resume(ctx context.Context, req *migratorpb.ResumeRequest) (*migratorpb.ResumeResponse, error) {
	session, ok := g.sessions.get(req.SessionId)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "session %s not found", req.SessionId)
	}

	session.mu.Lock()
	session.frozen = false
	session.mu.Unlock()

	log.Printf("[worker-%s] resumed session %s", g.workerID, req.SessionId)
	return &migratorpb.ResumeResponse{Ok: true}, nil
}

func (g *grpcServer) HealthStream(req *migratorpb.HealthStreamRequest, stream grpc.ServerStreamingServer[migratorpb.HealthUpdate]) error {
	ticker := time.NewTicker(g.healthInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-ticker.C:
			failures, written := g.stats.snapshotStats()

			err := stream.Send(&migratorpb.HealthUpdate{
				WorkerId:         g.workerID,
				SessionCount:     int32(g.sessions.count()),
				Status:           migratorpb.WorkerStatus_WORKER_STATUS_SERVING,
				UnixMs:           time.Now().UnixMilli(),
				SnapshotFailures: failures,
				SnapshotsWritten: written,
			})
			if err != nil {
				return err
			}
		}
	}
}
