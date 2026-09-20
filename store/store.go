package store

import (
	"context"
	"time"
)

type SessionData struct {
	ID          string
	WorkerID    string
	ConnectedAt time.Time
	Status      string // "active", "migrating", "migrated"
}

type Store interface {
	Save(ctx context.Context, data *SessionData) error
	Load(ctx context.Context, sessionID string) (*SessionData, error)
	Delete(ctx context.Context, sessionID string) error
	SaveSnapshot(ctx context.Context, sessionID string, state []byte) error
	LoadSnapshot(ctx context.Context, sessionID string) ([]byte, error)
	DeleteSnapshot(ctx context.Context, sessionID string) error
	Ping(ctx context.Context) error
}
