package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"time"

	"github.com/Jana-Hassan/ws-migrator/internal/frame"
	"github.com/Jana-Hassan/ws-migrator/store"
	"github.com/gorilla/websocket"
)

const (
	outboundBufferSize      = 1024
	ackInterval             = 20 * time.Millisecond
	migrateFallbackAttempts = 2

	pongWait   = 60 * time.Second
	pingPeriod = 50 * time.Second
	writeWait  = 10 * time.Second
)

type Server struct {
	pool       *Pool
	registry   *Registry
	store      store.Store
	grpcClient *GRPCClient
	metrics    Metrics

	registerToken string
}

var upgrader = websocket.Upgrader{ //converts http req to ws connection
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true //allow all (for now) -- is it allowed? (for later)
	},
}

func NewServer(pool *Pool, s store.Store) *Server {
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
	mux.HandleFunc("/admin/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(s.Stats()); err != nil {
			log.Printf("stats encode failed: %v", err)
		}
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

		resp, err := s.grpcClient.Checkpoint(r.Context(), session.WorkerID, sessionID, false)
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
		case session.migrateChannel <- migrateRequest{target: targetWorker}:
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
		migrateChannel: make(chan migrateRequest, 1),
	}
	s.registry.Add(session)
	s.metrics.sessionsStarted.Add(1)

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
	s.metrics.sessionsEnded.Add(1)
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
		s.metrics.sessionsRejected.Add(1)
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
		s.metrics.sessionsRejected.Add(1)
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
	return s.pickWorkerExcept(map[string]bool{exclude: true})
}

func (s *Server) pickWorkerExcept(exclude map[string]bool) (WorkerInfo, bool) {
	var best WorkerInfo
	found := false
	minSessions := -1

	for _, w := range s.pool.All() {
		if w.Draining || !w.Healthy || exclude[w.ID] {
			continue
		}
		count := s.registry.CountOnWorker(w.ID)
		if minSessions == -1 || count < minSessions {
			minSessions = count
			best = w
			found = true
		}
	}

	return best, found
}

type loadTracker struct {
	counts map[string]int
	ids    []string
	byID   map[string]WorkerInfo
}

func (s *Server) newLoadTracker(exclude string) *loadTracker {
	lt := &loadTracker{
		counts: make(map[string]int),
		byID:   make(map[string]WorkerInfo),
	}

	for _, w := range s.pool.All() {
		if w.Draining || !w.Healthy || w.ID == exclude {
			continue
		}
		lt.ids = append(lt.ids, w.ID)
		lt.byID[w.ID] = w
		lt.counts[w.ID] = s.registry.CountOnWorker(w.ID)
	}

	sort.Strings(lt.ids)
	return lt
}

func (lt *loadTracker) next() (WorkerInfo, bool) {
	best, lowest := "", -1
	for _, id := range lt.ids {
		if lowest == -1 || lt.counts[id] < lowest {
			best, lowest = id, lt.counts[id]
		}
	}

	if best == "" {
		return WorkerInfo{}, false
	}

	lt.counts[best]++
	return lt.byID[best], true
}

type clientMessage struct {
	messageType int
	data        []byte
}

func (s *Server) readClient(conn *websocket.Conn, out chan<- clientMessage, done <-chan struct{}) {
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

		select {
		case out <- clientMessage{messageType, data}:
		case <-done:
			return
		}
	}
}

func (s *Server) runSession(session *Session) {
	clientMessages := make(chan clientMessage)
	done := make(chan struct{})
	defer close(done)

	go s.readClient(session.ClientConn, clientMessages, done)

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
		s.pipeDownlink(session, workerConn)
		close(workerDone)
	}()

	acks := time.NewTicker(ackInterval)
	defer acks.Stop()
	var ackedReply, ackedPush uint64

	for {
		select {
		case <-acks.C:
			reply, push := session.watermarks()
			if reply == ackedReply && push == ackedPush {
				continue
			}
			if err := workerConn.WriteMessage(websocket.BinaryMessage, frame.EncodeAck(reply, push)); err != nil {
				log.Printf("session %s: ack to worker failed: %v", session.ID, err)
				continue
			}
			ackedReply, ackedPush = reply, push

		case msg, ok := <-clientMessages:
			if !ok {
				workerConn.Close()
				<-workerDone
				return false
			}

			buffered := session.bufferOutbound(msg.messageType, msg.data)
			if err := workerConn.WriteMessage(buffered.messageType, frame.EncodeUp(frame.Message, buffered.seq, buffered.data)); err == nil {
				s.metrics.messagesForwarded.Add(1)
			} else {
				log.Printf("session %s: write to worker failed: %v", session.ID, err)
				workerConn.Close()
				<-workerDone
				return s.recoverSession(session)
			}
			continue

		case <-workerDone:
			workerConn.Close()
			return s.recoverSession(session)

		case req := <-session.migrateChannel:
			if !s.migrateWithFallback(session, req, workerConn) {
				continue
			}
			workerConn.Close()
			<-workerDone
			return true
		}
	}
}

func (s *Server) pipeDownlink(session *Session, workerConn *websocket.Conn) {
	workerConn.SetReadDeadline(time.Now().Add(pongWait))
	workerConn.SetPongHandler(func(string) error {
		return workerConn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		messageType, message, err := workerConn.ReadMessage()
		if err != nil {
			log.Printf("worker→client: read stopped: %v", err)
			return
		}

		workerConn.SetReadDeadline(time.Now().Add(pongWait))

		kind, key, payload, ok := frame.DecodeDown(message)
		if !ok {
			log.Printf("session %s: malformed frame from worker, dropped", session.ID)
			continue
		}

		if !session.admitDownlink(kind, key) {
			s.metrics.duplicatesDropped.Add(1)
			continue
		}

		if err := session.ClientConn.WriteMessage(messageType, payload); err != nil {
			log.Printf("worker→client: write error: %v", err)
			return
		}
	}
}

func (s *Server) migrateWithFallback(session *Session, req migrateRequest, sourceConn *websocket.Conn) bool {
	session.mu.Lock()
	placed := session.WorkerID
	session.mu.Unlock()

	if req.target == placed {
		return false
	}

	if s.migrate(session, req.target, sourceConn) {
		return true
	}
	if !req.fallback {
		return false
	}

	session.mu.Lock()
	current := session.WorkerID
	session.mu.Unlock()

	tried := map[string]bool{req.target: true, current: true}
	for attempt := 0; attempt < migrateFallbackAttempts; attempt++ {
		target, ok := s.pickWorkerExcept(tried)
		if !ok {
			return false
		}

		tried[target.ID] = true
		log.Printf("session %s: retrying drain migration on %s", session.ID, target.ID)
		if s.migrate(session, target.ID, sourceConn) {
			return true
		}
	}

	return false
}

func (s *Server) migrate(session *Session, targetWorkerID string, sourceConn *websocket.Conn) bool {
	s.metrics.migrationsAttempted.Add(1)

	ok := s.doMigrate(session, targetWorkerID, sourceConn)
	if ok {
		s.metrics.migrationsSucceeded.Add(1)
	} else {
		s.metrics.migrationsFailed.Add(1)
	}
	return ok
}

func (s *Server) doMigrate(session *Session, targetWorkerID string, sourceConn *websocket.Conn) bool {
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

	source := session.WorkerID

	resp, err := s.grpcClient.Checkpoint(ctx, source, session.ID, true)
	if err != nil {
		log.Printf("migration failed: checkpoint error: %v", err)
		s.unfreeze(ctx, session, source, sourceConn, 0, true)
		return false
	}
	if !resp.Ok {
		log.Printf("migration failed: checkpoint not ok: %s", resp.Error)
		s.unfreeze(ctx, session, source, sourceConn, resp.AppliedSeq, true)
		return false
	}

	checkpoint := store.Snapshot{State: resp.State, AppliedSeq: resp.AppliedSeq, Outbox: resp.Outbox}

	if !s.placeOnWorker(ctx, session, target, checkpoint) {
		s.unfreeze(ctx, session, source, sourceConn, resp.AppliedSeq, true)
		return false
	}

	if err := s.store.SaveSnapshot(ctx, session.ID, checkpoint); err != nil {
		s.metrics.snapshotWriteFailures.Add(1)
		log.Printf("warning: could not snapshot session %s after migration: %v", session.ID, err)
	}

	log.Printf("migration complete: session %s now on %s", session.ID, targetWorkerID)
	return true
}

func (s *Server) unfreeze(ctx context.Context, session *Session, source string, sourceConn *websocket.Conn, appliedSeq uint64, resend bool) {
	if _, err := s.grpcClient.Resume(ctx, source, session.ID); err != nil {
		log.Printf("session %s: could not resume on %s: %v", session.ID, source, err)
		return
	}
	if resend {
		s.resendPending(session, sourceConn, appliedSeq)
	}
}

func (s *Server) placeOnWorker(ctx context.Context, session *Session, target WorkerInfo, snap store.Snapshot) bool {
	appliedSeq := snap.AppliedSeq
	resumeSeq := session.resumeAfter(appliedSeq)
	if resumeSeq != appliedSeq {
		s.metrics.resendGaps.Add(1)
		log.Printf("session %s: messages %d-%d fell out of the buffer and are lost", session.ID, appliedSeq+1, resumeSeq)
		appliedSeq = resumeSeq
	}

	restored, err := s.grpcClient.Restore(ctx, target.ID, session.ID, snap.State, appliedSeq, snap.Outbox)
	if err != nil {
		log.Printf("placement failed: restore error: %v", err)
		return false
	}
	if !restored.Ok {
		log.Printf("placement failed: restore not ok: %s", restored.Error)
		return false
	}

	workerURL := target.WSAddr + "?session_id=" + session.ID + "&resumed=1"
	workerConn, _, err := websocket.DefaultDialer.Dial(workerURL, nil)
	if err != nil {
		log.Printf("placement failed: could not dial %s: %v", target.ID, err)
		return false
	}

	workerConn.SetReadLimit(512 * 1024)

	if !s.resendPending(session, workerConn, appliedSeq) {
		workerConn.Close()
		return false
	}

	session.mu.Lock()
	session.WorkerConn = workerConn
	session.WorkerID = target.ID
	session.mu.Unlock()

	s.registry.UpdateWorker(session.ID, target.ID)
	return true
}

func (s *Server) resendPending(session *Session, workerConn *websocket.Conn, appliedSeq uint64) bool {
	pending, gap := session.outboundAfter(appliedSeq)
	if gap {
		s.metrics.resendGaps.Add(1)
		log.Printf("session %s: resend gap after seq %d, messages were dropped from the buffer", session.ID, appliedSeq)
	}

	for _, msg := range pending {
		if err := workerConn.WriteMessage(msg.messageType, frame.EncodeUp(frame.Message, msg.seq, msg.data)); err != nil {
			log.Printf("session %s: resend of seq %d failed: %v", session.ID, msg.seq, err)
			return false
		}
	}

	if len(pending) > 0 {
		s.metrics.resendsSent.Add(uint64(len(pending)))
		log.Printf("session %s: resent %d messages after seq %d", session.ID, len(pending), appliedSeq)
	}
	return true
}

func (s *Server) recoverSession(session *Session) bool {
	s.metrics.recoveriesAttempted.Add(1)

	ok := s.doRecoverSession(session)
	if ok {
		s.metrics.recoveriesSucceeded.Add(1)
	} else {
		s.metrics.recoveriesFailed.Add(1)
	}
	return ok
}

func (s *Server) doRecoverSession(session *Session) bool {
	ctx := context.Background()

	session.mu.Lock()
	deadWorker := session.WorkerID
	session.mu.Unlock()

	target, ok := s.pickWorker(deadWorker)
	if !ok {
		log.Printf("recovery failed: no healthy worker for session %s", session.ID)
		return false
	}

	snap, err := s.store.LoadSnapshot(ctx, session.ID)
	if err != nil {
		log.Printf("recovery failed: could not load snapshot for %s: %v", session.ID, err)
		return false
	}
	if snap == nil {
		snap = &store.Snapshot{}
	}

	log.Printf("recovering session %s from %s onto %s (%d state bytes, applied seq %d, %d outbox bytes)",
		session.ID, deadWorker, target.ID, len(snap.State), snap.AppliedSeq, len(snap.Outbox))

	if !s.placeOnWorker(ctx, session, target, *snap) {
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
