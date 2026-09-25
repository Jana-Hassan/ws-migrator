package gateway

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// info about one active client connection
type Session struct {
	ID             string
	WorkerID       string
	ClientConn     *websocket.Conn
	WorkerConn     *websocket.Conn
	ConnectedAt    time.Time
	mu             sync.Mutex
	migrateChannel chan string

	seq      uint64
	outbound []bufferedMessage
}

type bufferedMessage struct {
	seq         uint64
	messageType int
	data        []byte
}

func (s *Session) bufferOutbound(messageType int, data []byte) bufferedMessage {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.seq++
	msg := bufferedMessage{seq: s.seq, messageType: messageType, data: data}

	s.outbound = append(s.outbound, msg)
	if len(s.outbound) > outboundBufferSize {
		s.outbound = s.outbound[len(s.outbound)-outboundBufferSize:]
	}

	return msg
}

func (s *Session) outboundAfter(appliedSeq uint64) ([]bufferedMessage, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.outbound) == 0 {
		return nil, false
	}

	gap := s.outbound[0].seq > appliedSeq+1

	var pending []bufferedMessage
	for _, msg := range s.outbound {
		if msg.seq > appliedSeq {
			pending = append(pending, msg)
		}
	}
	return pending, gap
}

// tracks all active sessions in the gateway
type Registry struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

func NewRegistry() *Registry {
	return &Registry{
		sessions: make(map[string]*Session),
	}
}

func NewSessionID() string {
	return fmt.Sprintf("sess_%s", uuid.New().String())
}

func (r *Registry) Add(session *Session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sessions[session.ID] = session
}

// deletes a session when client disconnects
func (r *Registry) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.sessions, id)
}

// gets session by id
func (r *Registry) Get(id string) (*Session, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	session, ok := r.sessions[id]
	return session, ok
}

// returns all sessions currently on a specific worker
func (r *Registry) AllOnWorker(workerID string) []*Session {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []*Session
	for _, session := range r.sessions {
		if session.WorkerID == workerID {
			result = append(result, session)
		}
	}
	return result
}

// returns total # active sessions
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.sessions)
}

func (r *Registry) UpdateWorker(sessionID, workerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.sessions[sessionID]; ok {
		s.WorkerID = workerID
	}
}
