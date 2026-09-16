package worker

import (
	"sync"

	"github.com/gorilla/websocket"
)

type State interface {
	Serialize() ([]byte, error)
	Restore([]byte) error
}

type session struct {
	id   string
	conn *websocket.Conn
	state State
}

type sessionRegistry struct {
	mu       sync.RWMutex
	sessions map[string]*session
}

func newSessionRegistry() *sessionRegistry {
	return &sessionRegistry{
		sessions: make(map[string]*session),
	}
}

func (r *sessionRegistry) add(s *session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sessions[s.id] = s
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

func (r *sessionRegistry) count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.sessions)
}