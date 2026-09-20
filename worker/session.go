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
	mu      sync.Mutex
	writeMu sync.Mutex
	id      string
	conn    *websocket.Conn
	state   State
	dirty   bool
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
	pending  map[string]State
}

func newSessionRegistry() *sessionRegistry {
	return &sessionRegistry{
		sessions: make(map[string]*session),
		pending:  make(map[string]State),
	}
}

func (r *sessionRegistry) stage(id string, state State) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pending[id] = state
}

func (r *sessionRegistry) take(id string) (State, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.pending[id]
	delete(r.pending, id)
	return state, ok
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
