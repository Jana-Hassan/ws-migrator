package store

import (
	"context"
	"sync"
)

type snapshot struct {
	state      []byte
	appliedSeq uint64
}

type MemoryStore struct {
	mu        sync.RWMutex
	sessions  map[string]SessionData
	snapshots map[string]snapshot
}

func NewMemory() *MemoryStore {
	return &MemoryStore{
		sessions:  make(map[string]SessionData),
		snapshots: make(map[string]snapshot),
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

func (s *MemoryStore) SaveSnapshot(ctx context.Context, sessionID string, state []byte, appliedSeq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	stored := make([]byte, len(state))
	copy(stored, state)
	s.snapshots[sessionID] = snapshot{state: stored, appliedSeq: appliedSeq}
	return nil
}

func (s *MemoryStore) LoadSnapshot(ctx context.Context, sessionID string) ([]byte, uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stored, ok := s.snapshots[sessionID]
	if !ok {
		return nil, 0, nil
	}

	loaded := make([]byte, len(stored.state))
	copy(loaded, stored.state)
	return loaded, stored.appliedSeq, nil
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
