package store

import (
	"context"
	"sync"
)

type MemoryStore struct {
	mu        sync.RWMutex
	sessions  map[string]SessionData
	snapshots map[string][]byte
}

func NewMemory() *MemoryStore {
	return &MemoryStore{
		sessions:  make(map[string]SessionData),
		snapshots: make(map[string][]byte),
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

func (s *MemoryStore) SaveSnapshot(ctx context.Context, sessionID string, state []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	stored := make([]byte, len(state))
	copy(stored, state)
	s.snapshots[sessionID] = stored
	return nil
}

func (s *MemoryStore) LoadSnapshot(ctx context.Context, sessionID string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	state, ok := s.snapshots[sessionID]
	if !ok {
		return nil, nil
	}

	loaded := make([]byte, len(state))
	copy(loaded, state)
	return loaded, nil
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
