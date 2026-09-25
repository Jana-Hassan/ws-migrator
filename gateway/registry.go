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
	migrateChannel chan migrateRequest

	seq      uint64
	outbound []bufferedMessage
}

type migrateRequest struct {
	target   string
	fallback bool
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
	onWorker map[string]int
}

func NewRegistry() *Registry {
	return &Registry{
		sessions: make(map[string]*Session),
		onWorker: make(map[string]int),
	}
}

func NewSessionID() string {
	return fmt.Sprintf("sess_%s", uuid.New().String())
}

func (r *Registry) Add(session *Session) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.sessions[session.ID]; ok {
		r.onWorker[existing.WorkerID]--
	}

	r.sessions[session.ID] = session
	r.onWorker[session.WorkerID]++
}

// deletes a session when client disconnects
func (r *Registry) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	session, ok := r.sessions[id]
	if !ok {
		return
	}

	delete(r.sessions, id)
	if r.onWorker[session.WorkerID]--; r.onWorker[session.WorkerID] <= 0 {
		delete(r.onWorker, session.WorkerID)
	}
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

// counts sessions on a worker without scanning every session
func (r *Registry) CountOnWorker(workerID string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.onWorker[workerID]
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

	s, ok := r.sessions[sessionID]
	if !ok || s.WorkerID == workerID {
		return
	}

	if r.onWorker[s.WorkerID]--; r.onWorker[s.WorkerID] <= 0 {
		delete(r.onWorker, s.WorkerID)
	}
	r.onWorker[workerID]++
	s.WorkerID = workerID
}
