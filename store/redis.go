package store

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	sessionPrefix  = "session:"
	sessionTTL     = 24 * time.Hour
	snapshotPrefix = "snapshot:"
	redisTimeout   = time.Second
	callTimeout    = 2 * time.Second
	snapshotTTL    = 1 * time.Hour
)

type RedisStore struct {
	client *redis.Client
}

func NewRedis(addr string) *RedisStore {
	return &RedisStore{
		client: redis.NewClient(&redis.Options{
			Addr:         addr,
			DialTimeout:  redisTimeout,
			ReadTimeout:  redisTimeout,
			WriteTimeout: redisTimeout,
			MaxRetries:   1,
		}),
	}
}

func (s *RedisStore) key(sessionID string) string { //builds  redis key for a session
	return fmt.Sprintf("%s%s", sessionPrefix, sessionID)
}

func (s *RedisStore) snapshotKey(sessionID string) string {
	return fmt.Sprintf("%s%s", snapshotPrefix, sessionID)
}

func (s *RedisStore) SaveSnapshot(ctx context.Context, sessionID string, snap Snapshot) error {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	key := s.snapshotKey(sessionID)

	err := s.client.HSet(ctx, key, "state", snap.State, "applied_seq", snap.AppliedSeq, "outbox", snap.Outbox).Err()
	if err != nil {
		return fmt.Errorf("save snapshot %s: %w", sessionID, err)
	}

	if err := s.client.Expire(ctx, key, snapshotTTL).Err(); err != nil {
		return fmt.Errorf("set TTL for snapshot %s: %w", sessionID, err)
	}
	return nil
}

func (s *RedisStore) LoadSnapshot(ctx context.Context, sessionID string) (*Snapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	fields, err := s.client.HGetAll(ctx, s.snapshotKey(sessionID)).Result()
	if err != nil {
		return nil, fmt.Errorf("load snapshot %s: %w", sessionID, err)
	}
	if len(fields) == 0 {
		return nil, nil
	}

	appliedSeq, err := strconv.ParseUint(fields["applied_seq"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse applied_seq for snapshot %s: %w", sessionID, err)
	}

	return &Snapshot{
		State:      []byte(fields["state"]),
		AppliedSeq: appliedSeq,
		Outbox:     []byte(fields["outbox"]),
	}, nil
}

func (s *RedisStore) DeleteSnapshot(ctx context.Context, sessionID string) error {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	err := s.client.Del(ctx, s.snapshotKey(sessionID)).Err()
	if err != nil {
		return fmt.Errorf("delete snapshot %s: %w", sessionID, err)
	}
	return nil
}

func (s *RedisStore) Save(ctx context.Context, data *SessionData) error {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	key := s.key(data.ID)

	err := s.client.HSet(ctx, key,
		"id", data.ID,
		"worker_id", data.WorkerID,
		"connected_at", data.ConnectedAt.Format(time.RFC3339),
		"status", data.Status,
	).Err()
	if err != nil {
		return fmt.Errorf("save session %s: %w", data.ID, err)
	}

	err = s.client.Expire(ctx, key, sessionTTL).Err()
	if err != nil {
		return fmt.Errorf("set TTL for session %s: %w", data.ID, err)
	}

	return nil
}

// reads session data from redis by id
func (s *RedisStore) Load(ctx context.Context, sessionID string) (*SessionData, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	key := s.key(sessionID)

	fields, err := s.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("load session %s: %w", sessionID, err)
	}

	if len(fields) == 0 {
		return nil, nil
	}

	connectedAt, err := time.Parse(time.RFC3339, fields["connected_at"])
	if err != nil {
		return nil, fmt.Errorf("parse connected_at for session %s: %w", sessionID, err)
	}

	return &SessionData{
		ID:          fields["id"],
		WorkerID:    fields["worker_id"],
		ConnectedAt: connectedAt,
		Status:      fields["status"],
	}, nil
}

func (s *RedisStore) Delete(ctx context.Context, sessionID string) error {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	err := s.client.Del(ctx, s.key(sessionID)).Err()
	if err != nil {
		return fmt.Errorf("delete session %s: %w", sessionID, err)
	}
	return nil
}

// checks if redis is reachable
func (s *RedisStore) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	return s.client.Ping(ctx).Err()
}
