package gateway

import (
	"context"
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
}

var upgrader = websocket.Upgrader{   //converts http req to ws connection
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true       //allow all (for now) -- is it allowed? (for later)
	},
}

func NewServer(pool *Pool, s *store.Store) *Server {
	return &Server{
		pool:     pool,
		registry:   NewRegistry(),
		store:      s,
	}
}

func (s *Server) pipe(ctx context.Context, cancel context.CancelFunc, src *websocket.Conn, dst *websocket.Conn, direction string) {
	defer cancel() 

	go func() {
		<-ctx.Done()
		src.SetReadDeadline(time.Now())
	}()

	src.SetReadDeadline(time.Now().Add(pongWait))
	src.SetPongHandler(func(string) error {
		return src.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		messageType, message, err := src.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err,
				websocket.CloseGoingAway,
				websocket.CloseNormalClosure,
			) {
				log.Printf("%s: unexpected error: %v", direction, err)
			}
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
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.HandleClient)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	log.Printf("gateway starting on %s", addr)
	return http.ListenAndServe(addr, mux)
}

func (s *Server) startSession(id, workerID string, clientConn, workerConn *websocket.Conn) *Session {
	session := &Session{
		ID:          id,
		WorkerID:    workerID,
		ClientConn:  clientConn,
		WorkerConn:  workerConn,
		ConnectedAt: time.Now(),
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

worker, ok := s.pickWorker()
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
	defer workerConn.Close()

	log.Printf("connected to worker %s at %s", worker.ID, worker.WSAddr)

	clientConn.SetReadLimit(512 * 1024)
	workerConn.SetReadLimit(512 * 1024)

	session := s.startSession(sessionID, worker.ID,clientConn, workerConn)
	defer s.endSession(session)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.keepAlive(ctx, clientConn, "client")
	go s.keepAlive(ctx, workerConn, "worker")

	go s.pipe(ctx, cancel, clientConn, workerConn, "client→worker")
	s.pipe(ctx, cancel, workerConn, clientConn, "worker→client")

	log.Printf("connection closed for %s", r.RemoteAddr)
}

func (s *Server) pickWorker() (*WorkerInfo, bool) {
	workers := s.pool.All()
	if len(workers) == 0 {
		return nil, false
	}

	var best *WorkerInfo
	minSessions := -1

	for _, w := range workers {
		count := len(s.registry.AllOnWorker(w.ID))
		if minSessions == -1 || count < minSessions {
			minSessions = count
			best = w
		}
	}

	return best, true
}
