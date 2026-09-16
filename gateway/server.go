package gateway

import (
	"context"
	"net/http"
	"time"
	"log"

	"github.com/gorilla/websocket"   //handles websocket protocol
	"github.com/Jana-Hassan/ws-migrator/store"

)

type Server struct {
	workerAddr string
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

func NewServer(workerAddr string, s *store.Store) *Server {
	return &Server{
		workerAddr: workerAddr,
		registry:   NewRegistry(),
		store:      s,
	}
}


func (s *Server) pipe(ctx context.Context, cancel context.CancelFunc, src *websocket.Conn, dst *websocket.Conn, direction string,) {
    defer cancel() //when this pipe stops, stop the other one too
  go func() {
        <-ctx.Done()
        src.SetReadDeadline(time.Now()) // expire immediately → unblocks ReadMessage
    }()

    for {
        src.SetReadDeadline(time.Now().Add(60 * time.Second))

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
        err = dst.WriteMessage(messageType, message)
        if err != nil {
            log.Printf("%s: write error: %v", direction, err)
            return
        }

        log.Printf("%s: forwarded %d bytes", direction, len(message))
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

func (s *Server) startSession(id string, clientConn, workerConn *websocket.Conn) *Session {
	session := &Session{
		ID:          id,
		WorkerID:    "worker-1",
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

	log.Printf("session %s started, total active: %d", session.ID, s.registry.Count())
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

	//generate session id first then pass it to the worker
	sessionID := NewSessionID()

	//dial worker with session id
	workerURL := s.workerAddr + "?session_id=" + sessionID
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

	log.Printf("connected to worker at %s", s.workerAddr)

	clientConn.SetReadLimit(512 * 1024)
	workerConn.SetReadLimit(512 * 1024)

	//create session with pre generated id
	session := s.startSession(sessionID, clientConn, workerConn)
	defer s.endSession(session)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.pipe(ctx, cancel, clientConn, workerConn, "client→worker")
	s.pipe(ctx, cancel, workerConn, clientConn, "worker→client")

	log.Printf("connection closed for %s", r.RemoteAddr)
}