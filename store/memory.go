package store

import (
	"context"
	"sync"
)

type MemoryStore struct {
	mu        sync.RWMutex
	sessions  map[string]SessionData
	snapshots map[string]Snapshot
}

func NewMemory() *MemoryStore {
	return &MemoryStore{
		sessions:  make(map[string]SessionData),
		snapshots: make(map[string]Snapshot),
	}
}

func (s *MemoryStore) Save(ctx context.Context, data *SessionData) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[data.ID] = *data
	return nil
}

func (s *MemoryStore) Load(ctx context.Context, sessionID string) (*SessionData, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := s.sessions[sessionID]
	if !ok {
		return nil, nil
	}
	return &data, nil
}

func (s *MemoryStore) Delete(ctx context.Context, sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, sessionID)
	return nil
}

func (s *MemoryStore) SaveSnapshot(ctx context.Context, sessionID string, snap Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.snapshots[sessionID] = Snapshot{
		State:      append([]byte(nil), snap.State...),
		AppliedSeq: snap.AppliedSeq,
		Outbox:     append([]byte(nil), snap.Outbox...),
	}
	return nil
}

func (s *MemoryStore) LoadSnapshot(ctx context.Context, sessionID string) (*Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stored, ok := s.snapshots[sessionID]
	if !ok {
		return nil, nil
	}

	return &Snapshot{
		State:      append([]byte(nil), stored.State...),
		AppliedSeq: stored.AppliedSeq,
		Outbox:     append([]byte(nil), stored.Outbox...),
	}, nil
}

func (s *MemoryStore) DeleteSnapshot(ctx context.Context, sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.snapshots, sessionID)
	return nil
}

func (s *MemoryStore) Ping(ctx context.Context) error {
	return nil
}
