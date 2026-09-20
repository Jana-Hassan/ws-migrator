package gateway

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/Jana-Hassan/ws-migrator/store"
	"github.com/gorilla/websocket"
)

const (
	pongWait   = 60 * time.Second
	pingPeriod = 50 * time.Second
	writeWait  = 10 * time.Second
)

type Server struct {
	pool       *Pool
	registry   *Registry
	store      *store.Store
	grpcClient *GRPCClient
}

var upgrader = websocket.Upgrader{ //converts http req to ws connection
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true //allow all (for now) -- is it allowed? (for later)
	},
}

func NewServer(pool *Pool, s *store.Store) *Server {
	return &Server{
		pool:       pool,
		registry:   NewRegistry(),
		store:      s,
		grpcClient: NewGRPCClient(pool),
	}
}

func (s *Server) keepAlive(ctx context.Context, conn *websocket.Conn, name string) {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeWait))
			if err != nil {
				log.Printf("%s: ping error: %v", name, err)
				return
			}
		}
	}
}

func (s *Server) Start(addr string) error {
	log.Printf("gateway starting on %s", addr)
	return http.ListenAndServe(addr, s.Handler())
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.HandleClient)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/admin/checkpoint", func(w http.ResponseWriter, r *http.Request) {
		sessionID := r.URL.Query().Get("session_id")
		if sessionID == "" {
			http.Error(w, "missing session_id", http.StatusBadRequest)
			return
		}

		session, ok := s.registry.Get(sessionID)
		if !ok {
			http.Error(w, "session not found", http.StatusNotFound)
			return
		}

		resp, err := s.grpcClient.Checkpoint(r.Context(), session.WorkerID, sessionID)
		if err != nil {
			http.Error(w, fmt.Sprintf("checkpoint failed: %v", err), http.StatusInternalServerError)
			return
		}

		fmt.Fprintf(w, "checkpoint ok=%v state_bytes=%d\n", resp.Ok, len(resp.State))
	})

	mux.HandleFunc("/admin/migrate", func(w http.ResponseWriter, r *http.Request) {
		sessionID := r.URL.Query().Get("session_id")
		targetWorker := r.URL.Query().Get("worker")
		if sessionID == "" || targetWorker == "" {
			http.Error(w, "missing session_id or worker", http.StatusBadRequest)
			return
		}

		session, ok := s.registry.Get(sessionID)
		if !ok {
			http.Error(w, "session not found", http.StatusNotFound)
			return
		}

		select {
		case session.migrateChannel <- targetWorker:
			fmt.Fprintf(w, "migration triggered for %s to %s\n", sessionID, targetWorker)
		default:
			http.Error(w, "migration already in progress", http.StatusConflict)
		}
	})

	return mux
}

func (s *Server) startSession(id, workerID string, clientConn, workerConn *websocket.Conn) *Session {
	session := &Session{
		ID:             id,
		WorkerID:       workerID,
		ClientConn:     clientConn,
		WorkerConn:     workerConn,
		ConnectedAt:    time.Now(),
		migrateChannel: make(chan string, 1),
	}
	s.registry.Add(session)

	err := s.store.Save(context.Background(), &store.SessionData{
		ID:          session.ID,
		WorkerID:    session.WorkerID,
		ConnectedAt: session.ConnectedAt,
		Status:      "active",
	})
	if err != nil {
		log.Printf("failed to save session %s to Redis: %v", session.ID, err)
	}

	log.Printf("session %s started on %s, total active: %d", session.ID, workerID, s.registry.Count())
	return session
}

func (s *Server) endSession(session *Session) {
	s.registry.Remove(session.ID)
	err := s.store.Delete(context.Background(), session.ID)
	if err != nil {
		log.Printf("failed to delete session %s from Redis: %v", session.ID, err)
	}
	if err := s.store.DeleteSnapshot(context.Background(), session.ID); err != nil {
		log.Printf("failed to delete snapshot %s from Redis: %v", session.ID, err)
	}
	log.Printf("session %s ended, total active: %d", session.ID, s.registry.Count())
}

func (s *Server) HandleClient(w http.ResponseWriter, r *http.Request) {
	clientConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("failed to upgrade client connection: %v", err)
		return
	}
	defer clientConn.Close()

	log.Printf("client connected from %s", r.RemoteAddr)

	sessionID := NewSessionID()

	worker, ok := s.pickWorker("")
	if !ok {
		log.Printf("no workers available")
		clientConn.WriteMessage(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(
				websocket.CloseInternalServerErr,
				"no worker available",
			),
		)
		return
	}

	workerURL := worker.WSAddr + "?session_id=" + sessionID
	workerConn, _, err := websocket.DefaultDialer.Dial(workerURL, nil)
	if err != nil {
		log.Printf("failed to connect to worker: %v", err)
		clientConn.WriteMessage(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(
				websocket.CloseInternalServerErr,
				"no worker available",
			),
		)
		return
	}

	log.Printf("connected to worker %s at %s", worker.ID, worker.WSAddr)

	clientConn.SetReadLimit(512 * 1024)
	workerConn.SetReadLimit(512 * 1024)

	session := s.startSession(sessionID, worker.ID, clientConn, workerConn)
	defer s.endSession(session)

	s.runSession(session)

	log.Printf("connection closed for %s", r.RemoteAddr)
}

func (s *Server) pickWorker(exclude string) (WorkerInfo, bool) {
	var best WorkerInfo
	found := false
	minSessions := -1

	for _, w := range s.pool.All() {
		if w.Draining || !w.Healthy || w.ID == exclude {
			continue
		}
		count := len(s.registry.AllOnWorker(w.ID))
		if minSessions == -1 || count < minSessions {
			minSessions = count
			best = w
			found = true
		}
	}

	return best, found
}

type clientMessage struct {
	messageType int
	data        []byte
}

func (s *Server) readClient(conn *websocket.Conn, out chan<- clientMessage) {
	defer close(out)
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(pongWait))
	})
	for {
		messageType, data, err := conn.ReadMessage()
		if err != nil {
			log.Printf("client: read stopped: %v", err)
			return
		}
		conn.SetReadDeadline(time.Now().Add(pongWait))
		out <- clientMessage{messageType, data}
	}
}

func (s *Server) runSession(session *Session) {
	clientMessages := make(chan clientMessage)
	go s.readClient(session.ClientConn, clientMessages)

	for {
		session.mu.Lock()
		workerConn := session.WorkerConn
		session.mu.Unlock()

		log.Printf("session %s: serving on worker %s", session.ID, session.WorkerID)

		ctx, cancel := context.WithCancel(context.Background())

		go s.keepAlive(ctx, session.ClientConn, "client")
		go s.keepAlive(ctx, workerConn, "worker")

		migrated := s.runOnWorker(cancel, session, workerConn, clientMessages)

		log.Printf("session %s: stopped serving, migrated=%v", session.ID, migrated)

		if !migrated {
			workerConn.Close()
			return
		}
	}
}

func (s *Server) runOnWorker(cancel context.CancelFunc, session *Session, workerConn *websocket.Conn, clientMessages <-chan clientMessage) bool {
	defer cancel()

	workerDone := make(chan struct{})
	go func() {
		s.pipeMessages(workerConn, session.ClientConn, "worker→client")
		close(workerDone)
	}()

	for {
		select {
		case msg, ok := <-clientMessages:
			if ok && workerConn.WriteMessage(msg.messageType, msg.data) == nil {
				continue
			}
			workerConn.Close()
			<-workerDone
			return false

		case <-workerDone:
			workerConn.Close()
			return s.recoverSession(session)

		case targetWorkerID := <-session.migrateChannel:
			if !s.migrate(session, targetWorkerID) {
				continue
			}
			workerConn.Close()
			<-workerDone
			return true
		}
	}
}

func (s *Server) pipeMessages(src, dst *websocket.Conn, direction string) {
	src.SetReadDeadline(time.Now().Add(pongWait))
	src.SetPongHandler(func(string) error {
		return src.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		messageType, message, err := src.ReadMessage()
		if err != nil {
			log.Printf("%s: read stopped: %v", direction, err)
			return
		}

		src.SetReadDeadline(time.Now().Add(pongWait))

		err = dst.WriteMessage(messageType, message)
		if err != nil {
			log.Printf("%s: write error: %v", direction, err)
			return
		}

		log.Printf("%s: forwarded %d bytes", direction, len(message))
	}
}

func (s *Server) migrate(session *Session, targetWorkerID string) bool {
	log.Printf("migrating session %s from %s to %s", session.ID, session.WorkerID, targetWorkerID)
	if targetWorkerID == session.WorkerID {
		log.Printf("migration failed: session %s already on %s", session.ID, targetWorkerID)
		return false
	}

	target, ok := s.pool.Get(targetWorkerID)
	if !ok {
		log.Printf("migration failed: target worker %s not in pool", targetWorkerID)
		return false
	}
	if target.Draining {
		log.Printf("migration failed: target worker %s is draining", targetWorkerID)
		return false
	}

	ctx := context.Background()

	s.saveSession(ctx, session, "migrating")
	defer s.saveSession(ctx, session, "active")

	resp, err := s.grpcClient.Checkpoint(ctx, session.WorkerID, session.ID)
	if err != nil {
		log.Printf("migration failed: checkpoint error: %v", err)
		return false
	}
	if !resp.Ok {
		log.Printf("migration failed: checkpoint not ok: %s", resp.Error)
		return false
	}

	if !s.placeOnWorker(ctx, session, target, resp.State) {
		return false
	}

	if err := s.store.SaveSnapshot(ctx, session.ID, resp.State); err != nil {
		log.Printf("warning: could not snapshot session %s after migration: %v", session.ID, err)
	}

	log.Printf("migration complete: session %s now on %s", session.ID, targetWorkerID)
	return true
}

func (s *Server) placeOnWorker(ctx context.Context, session *Session, target WorkerInfo, state []byte) bool {
	restored, err := s.grpcClient.Restore(ctx, target.ID, session.ID, state)
	if err != nil {
		log.Printf("placement failed: restore error: %v", err)
		return false
	}
	if !restored.Ok {
		log.Printf("placement failed: restore not ok: %s", restored.Error)
		return false
	}

	workerURL := target.WSAddr + "?session_id=" + session.ID
	workerConn, _, err := websocket.DefaultDialer.Dial(workerURL, nil)
	if err != nil {
		log.Printf("placement failed: could not dial %s: %v", target.ID, err)
		return false
	}

	workerConn.SetReadLimit(512 * 1024)

	session.mu.Lock()
	session.WorkerConn = workerConn
	session.WorkerID = target.ID
	session.mu.Unlock()

	s.registry.UpdateWorker(session.ID, target.ID)
	return true
}

func (s *Server) recoverSession(session *Session) bool {
	ctx := context.Background()

	session.mu.Lock()
	deadWorker := session.WorkerID
	session.mu.Unlock()

	target, ok := s.pickWorker(deadWorker)
	if !ok {
		log.Printf("recovery failed: no healthy worker for session %s", session.ID)
		return false
	}

	state, err := s.store.LoadSnapshot(ctx, session.ID)
	if err != nil {
		log.Printf("recovery failed: could not load snapshot for %s: %v", session.ID, err)
		return false
	}

	log.Printf("recovering session %s from %s onto %s (%d state bytes)", session.ID, deadWorker, target.ID, len(state))

	if !s.placeOnWorker(ctx, session, target, state) {
		return false
	}

	s.saveSession(ctx, session, "active")
	log.Printf("recovery complete: session %s now on %s", session.ID, target.ID)
	return true
}

func (s *Server) saveSession(ctx context.Context, session *Session, status string) {
	session.mu.Lock()
	data := &store.SessionData{
		ID:          session.ID,
		WorkerID:    session.WorkerID,
		ConnectedAt: session.ConnectedAt,
		Status:      status,
	}
	session.mu.Unlock()

	if err := s.store.Save(ctx, data); err != nil {
		log.Printf("failed to save session %s as %s: %v", session.ID, status, err)
	}
}
