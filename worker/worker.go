package worker

import (
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	migratorpb "github.com/Jana-Hassan/ws-migrator/proto"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
)

const (
	defaultDrainTimeout = 30 * time.Second
	drainPollInterval   = 500 * time.Millisecond
)

type Worker struct {
	id           string
	wsAddr       string
	grpcAddr     string
	gatewayAddr  string
	drainTimeout time.Duration
	sessions     *sessionRegistry
	onMessage    func(sessionID string, state State, msg []byte) []byte
	onCreate     func(sessionID string) State
}

type Config struct {
	ID           string
	GRPCAddr     string
	WSAddr       string
	GatewayAddr  string
	DrainTimeout time.Duration
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func NewWorker(cfg Config) *Worker {
	if cfg.DrainTimeout == 0 {
		cfg.DrainTimeout = defaultDrainTimeout
	}

	return &Worker{
		id:           cfg.ID,
		wsAddr:       cfg.WSAddr,
		grpcAddr:     cfg.GRPCAddr,
		gatewayAddr:  cfg.GatewayAddr,
		drainTimeout: cfg.DrainTimeout,
		sessions:     newSessionRegistry(),
	}
}

func (w *Worker) OnMessage(fn func(sessionID string, state State, msg []byte) []byte) {
	w.onMessage = fn
}
//optionally provides this
func (w *Worker) OnSessionCreate(fn func(sessionID string) State) {
	w.onCreate = fn
}

func (w *Worker) Start() error {
	errCh := make(chan error, 2)

	go func() {
		errCh <- w.startGRPC()
	}()

	go func() {
		errCh <- w.startWS()
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, os.Interrupt)

	select {
	case err := <-errCh:
		return err
	case sig := <-signals:
		w.Drain(sig.String())
		return nil
	}
}

func (w *Worker) Drain(reason string) {
	if w.gatewayAddr == "" {
		log.Printf("[worker-%s] no gateway address set, shutting down with %d sessions", w.id, w.sessions.count())
		return
	}

	client, err := newGatewayClient(w.gatewayAddr)
	if err != nil {
		log.Printf("[worker-%s] drain failed: %v", w.id, err)
		return
	}
	defer client.close()

	deadline := time.Now().Add(w.drainTimeout)
	for {
		resp, err := client.drain(w.id, reason)
		if err != nil {
			log.Printf("[worker-%s] drain call failed: %v", w.id, err)
		} else if !resp.Accepted {
			log.Printf("[worker-%s] gateway could not place all %d sessions", w.id, resp.SessionCount)
		}

		if w.sessions.count() == 0 {
			log.Printf("[worker-%s] drained, shutting down", w.id)
			return
		}
		if time.Now().After(deadline) {
			break
		}

		time.Sleep(drainPollInterval)
	}

	log.Printf("[worker-%s] drain timeout, %d sessions still active", w.id, w.sessions.count())
}

func (w *Worker) startGRPC() error {
	lis, err := net.Listen("tcp", w.grpcAddr)
	if err != nil {
		return err
	}

	log.Printf("[worker-%s] gRPC server starting on %s", w.id, w.grpcAddr)
	return w.StartGRPCOn(lis)
}

func (w *Worker) StartGRPCOn(lis net.Listener) error {
	srv := grpc.NewServer()
	migratorpb.RegisterWorkerServiceServer(srv, newGRPCServer(w.id, w.sessions, w.onCreate))
	return srv.Serve(lis)
}

func (w *Worker) startWS() error {
	log.Printf("[worker-%s] WebSocket server starting on %s", w.id, w.wsAddr)
	return http.ListenAndServe(w.wsAddr, w.Handler())
}

func (w *Worker) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", w.handleConnection)
	mux.HandleFunc("/health", func(wr http.ResponseWriter, r *http.Request) {
		wr.WriteHeader(http.StatusOK)
		wr.Write([]byte("ok"))
	})

	return mux
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

	if state, ok := w.sessions.take(sessionID); ok {
		s.state = state
	} else if w.onCreate != nil {
		s.state = w.onCreate(sessionID)
	}

	if err := w.sessions.add(s); err != nil {
		log.Printf("[worker-%s] rejected session %s: %v", w.id, sessionID, err)
		conn.Close()
		return
	}

	defer conn.Close()
	defer w.sessions.remove(sessionID)

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

		s.mu.Lock()
		var response []byte
		if w.onMessage != nil {
			response = w.onMessage(sessionID, s.state, msg)
		} else {
			response = msg
		}
		s.mu.Unlock()

		if err := conn.WriteMessage(messageType, response); err != nil {
			log.Printf("[worker-%s] write error: %v", w.id, err)
			return
		}
	}
}
