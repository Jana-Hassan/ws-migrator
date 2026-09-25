package worker

import (
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type State interface {
	Serialize() ([]byte, error)
	Restore([]byte) error
}

type session struct {
	mu         sync.Mutex
	writeMu    sync.Mutex
	id         string
	conn       *websocket.Conn
	state      State
	dirty      bool
	appliedSeq uint64
}

func (s *session) write(messageType int, data []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	s.conn.SetWriteDeadline(time.Now().Add(writeWait))
	return s.conn.WriteMessage(messageType, data)
}

type sessionRegistry struct {
	mu       sync.RWMutex
	sessions map[string]*session
	pending  map[string]pendingState
}

type pendingState struct {
	state      State
	stagedAt   time.Time
	appliedSeq uint64
}

func newSessionRegistry() *sessionRegistry {
	return &sessionRegistry{
		sessions: make(map[string]*session),
		pending:  make(map[string]pendingState),
	}
}

func (r *sessionRegistry) stage(id string, state State, appliedSeq uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pending[id] = pendingState{state: state, stagedAt: time.Now(), appliedSeq: appliedSeq}
}

func (r *sessionRegistry) sweepPending(ttl time.Duration) []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	var dropped []string
	for id, p := range r.pending {
		if time.Since(p.stagedAt) >= ttl {
			delete(r.pending, id)
			dropped = append(dropped, id)
		}
	}
	return dropped
}

func (r *sessionRegistry) take(id string) (State, uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.pending[id]
	delete(r.pending, id)
	return p.state, p.appliedSeq, ok
}

func (r *sessionRegistry) add(s *session) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.sessions[s.id]; ok {
		return fmt.Errorf("session %s already exists", s.id)
	}
	r.sessions[s.id] = s
	return nil
}

func (r *sessionRegistry) remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.sessions, id)
}

func (r *sessionRegistry) get(id string) (*session, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.sessions[id]
	return s, ok
}

func (r *sessionRegistry) all() []*session {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*session, 0, len(r.sessions))
	for _, s := range r.sessions {
		result = append(result, s)
	}
	return result
}

func (r *sessionRegistry) count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.sessions)
}
