package worker

import (
	"log"
	"net"
	"net/http"

	migratorpb "github.com/Jana-Hassan/ws-migrator/proto"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
)

type Worker struct {
	id        string
	sessions  *sessionRegistry
	onMessage func(sessionID string, msg []byte) []byte
	onCreate  func(sessionID string) State
}

type Config struct {
	ID       string
	GRPCAddr string
	WSAddr   string
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func NewWorker(cfg Config) *Worker {
	return &Worker{
		id:       cfg.ID,
		sessions: newSessionRegistry(),
	}
}

func (w *Worker) OnMessage(fn func(sessionID string, msg []byte) []byte) {
	w.onMessage = fn
}
//optionally provides this
func (w *Worker) OnSessionCreate(fn func(sessionID string) State) {
	w.onCreate = fn
}

func (w *Worker) Start(cfg Config) error {
	grpcErrCh := make(chan error, 1)
	go func() {
		grpcErrCh <- w.startGRPC(cfg.GRPCAddr)
	}()

	go func() {
		if err := <-grpcErrCh; err != nil {
			log.Fatalf("[worker-%s] gRPC server failed: %v", w.id, err)
		}
	}()

	return w.startWS(cfg.WSAddr)
}

func (w *Worker) startGRPC(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	srv := grpc.NewServer()
	migratorpb.RegisterWorkerServiceServer(srv, newGRPCServer(w.id, w.sessions))

	log.Printf("[worker-%s] gRPC server starting on %s", w.id, addr)
	return srv.Serve(lis)
}

func (w *Worker) startWS(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", w.handleConnection)
	mux.HandleFunc("/health", func(wr http.ResponseWriter, r *http.Request) {
		wr.WriteHeader(http.StatusOK)
		wr.Write([]byte("ok"))
	})

	log.Printf("[worker-%s] WebSocket server starting on %s", w.id, addr)
	return http.ListenAndServe(addr, mux)
}

func (w *Worker) handleConnection(wr http.ResponseWriter, r *http.Request) {
	sessionID := r.URL.Query().Get("session_id")
	if sessionID == "" {
		http.Error(wr, "missing session_id", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(wr, r, nil)
	if err != nil {
		log.Printf("[worker-%s] upgrade failed: %v", w.id, err)
		return
	}

	s := &session{
		id:   sessionID,
		conn: conn,
	}

	if w.onCreate != nil {
		s.state = w.onCreate(sessionID)
	}

	w.sessions.add(s)
	defer w.sessions.remove(sessionID)
	defer conn.Close()

	log.Printf("[worker-%s] session %s connected", w.id, sessionID)

	for {
		messageType, msg, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err,
				websocket.CloseGoingAway,
				websocket.CloseNormalClosure,
			) {
				log.Printf("[worker-%s] unexpected error: %v", w.id, err)
			}
			log.Printf("[worker-%s] session %s disconnected", w.id, sessionID)
			return
		}

		var response []byte
		if w.onMessage != nil {
			response = w.onMessage(sessionID, msg)
		} else {
			response = msg
		}

		if err := conn.WriteMessage(messageType, response); err != nil {
			log.Printf("[worker-%s] write error: %v", w.id, err)
			return
		}
	}
}