package gateway

import (
	"fmt"
	"sync"
	"time"

	"github.com/Jana-Hassan/ws-migrator/internal/frame"
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

	lastReply uint64
	lastPush  uint64
}

func (s *Session) watermarks() (reply, push uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastReply, s.lastPush
}

func (s *Session) admitDownlink(kind byte, key uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch kind {
	case frame.Reply:
		if key <= s.lastReply {
			return false
		}
		s.lastReply = key
	case frame.Push:
		if key <= s.lastPush {
			return false
		}
		s.lastPush = key
	}
	return true
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

func (s *Session) resumeAfter(appliedSeq uint64) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.outbound) > 0 && s.outbound[0].seq > appliedSeq+1 {
		return s.outbound[0].seq - 1
	}
	return appliedSeq
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
	workerOf map[string]string
	onWorker map[string]int
}

func NewRegistry() *Registry {
	return &Registry{
		sessions: make(map[string]*Session),
		workerOf: make(map[string]string),
		onWorker: make(map[string]int),
	}
}

func (r *Registry) detach(sessionID string) {
	worker, ok := r.workerOf[sessionID]
	if !ok {
		return
	}

	delete(r.workerOf, sessionID)
	if r.onWorker[worker]--; r.onWorker[worker] <= 0 {
		delete(r.onWorker, worker)
	}
}

func (r *Registry) attach(sessionID, workerID string) {
	r.workerOf[sessionID] = workerID
	r.onWorker[workerID]++
}

func NewSessionID() string {
	return fmt.Sprintf("sess_%s", uuid.New().String())
}

func (r *Registry) Add(session *Session) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.detach(session.ID)
	r.sessions[session.ID] = session
	r.attach(session.ID, session.WorkerID)
}

// deletes a session when client disconnects
func (r *Registry) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.sessions, id)
	r.detach(id)
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
	for id, session := range r.sessions {
		if r.workerOf[id] == workerID {
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

	if _, ok := r.sessions[sessionID]; !ok {
		return
	}
	if r.workerOf[sessionID] == workerID {
		return
	}

	r.detach(sessionID)
	r.attach(sessionID, workerID)
}
