package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Jana-Hassan/ws-migrator/internal/frame"
	migratorpb "github.com/Jana-Hassan/ws-migrator/proto"
	"github.com/Jana-Hassan/ws-migrator/store"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
)

const (
	defaultDrainTimeout      = 30 * time.Second
	defaultDrainPollInterval = 100 * time.Millisecond
	defaultHealthInterval    = 5 * time.Second
	defaultSnapshotInterval  = 5 * time.Second
	writeWait                = 10 * time.Second
	defaultRegisterInterval  = 30 * time.Second
	defaultPendingTTL        = 60 * time.Second
)

type Worker struct {
	id               string
	wsAddr           string
	grpcAddr         string
	gatewayAddr      string
	drainTimeout     time.Duration
	drainPoll        time.Duration
	healthInterval   time.Duration
	snapshotInterval time.Duration
	registerInterval time.Duration
	pendingTTL       time.Duration
	registerToken    string
	advertiseWS      string
	advertiseGRPC    string
	draining         chan struct{}
	drainOnce        sync.Once
	snapshotFailures atomic.Uint64
	snapshotsWritten atomic.Uint64
	store            store.Store
	sessions         *sessionRegistry
	onMessage        func(sessionID string, state State, msg []byte) []byte
	onCreate         func(sessionID string) State
	onEnd            func(sessionID string, state State)
	onReady          func(Ready)
}

var ErrFrozen = errors.New("session is being handed over, retry shortly")

type Ready struct {
	SessionID string
	State     State
	Resumed   bool
}

type Config struct {
	ID                string
	GRPCAddr          string
	WSAddr            string
	GatewayAddr       string
	DrainTimeout      time.Duration
	DrainPollInterval time.Duration
	HealthInterval    time.Duration
	SnapshotInterval  time.Duration
	RegisterInterval  time.Duration
	PendingTTL        time.Duration
	RegisterToken     string
	AdvertiseWS       string
	AdvertiseGRPC     string
	Store             store.Store
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
	if cfg.DrainPollInterval == 0 {
		cfg.DrainPollInterval = defaultDrainPollInterval
	}
	if cfg.HealthInterval == 0 {
		cfg.HealthInterval = defaultHealthInterval
	}
	if cfg.SnapshotInterval == 0 {
		cfg.SnapshotInterval = defaultSnapshotInterval
	}
	if cfg.RegisterInterval == 0 {
		cfg.RegisterInterval = defaultRegisterInterval
	}
	if cfg.PendingTTL == 0 {
		cfg.PendingTTL = defaultPendingTTL
	}
	if cfg.AdvertiseWS == "" {
		cfg.AdvertiseWS = advertised("ws://", cfg.WSAddr, "/ws")
	}
	if cfg.AdvertiseGRPC == "" {
		cfg.AdvertiseGRPC = advertised("", cfg.GRPCAddr, "")
	}

	return &Worker{
		id:               cfg.ID,
		wsAddr:           cfg.WSAddr,
		grpcAddr:         cfg.GRPCAddr,
		gatewayAddr:      cfg.GatewayAddr,
		drainTimeout:     cfg.DrainTimeout,
		drainPoll:        cfg.DrainPollInterval,
		healthInterval:   cfg.HealthInterval,
		snapshotInterval: cfg.SnapshotInterval,
		registerInterval: cfg.RegisterInterval,
		pendingTTL:       cfg.PendingTTL,
		registerToken:    cfg.RegisterToken,
		advertiseWS:      cfg.AdvertiseWS,
		advertiseGRPC:    cfg.AdvertiseGRPC,
		draining:         make(chan struct{}),
		store:            cfg.Store,
		sessions:         newSessionRegistry(),
	}
}

func (w *Worker) snapshotStats() (failures, written uint64) {
	return w.snapshotFailures.Load(), w.snapshotsWritten.Load()
}

func (w *Worker) OnMessage(fn func(sessionID string, state State, msg []byte) []byte) {
	w.onMessage = fn
}

// optionally provides this
func (w *Worker) OnSessionCreate(fn func(sessionID string) State) {
	w.onCreate = fn
}

func (w *Worker) OnSessionEnd(fn func(sessionID string, state State)) {
	w.onEnd = fn
}

func (w *Worker) OnSessionReady(fn func(Ready)) {
	w.onReady = fn
}

func (w *Worker) Update(sessionID string, fn func(State)) error {
	s, ok := w.sessions.get(sessionID)
	if !ok {
		return fmt.Errorf("session %s not on worker %s", sessionID, w.id)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.frozen {
		return ErrFrozen
	}

	fn(s.state)
	s.dirty = true
	return nil
}

func (w *Worker) Send(sessionID string, msg []byte) error {
	s, ok := w.sessions.get(sessionID)
	if !ok {
		return fmt.Errorf("session %s not on worker %s", sessionID, w.id)
	}

	return s.write(websocket.TextMessage, frame.EncodeDown(frame.Plain, 0, msg))
}

func (w *Worker) Push(sessionID string, fn func(State) (key uint64, msg []byte, ok bool)) error {
	s, ok := w.sessions.get(sessionID)
	if !ok {
		return fmt.Errorf("session %s not on worker %s", sessionID, w.id)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.frozen {
		return ErrFrozen
	}

	key, msg, ok := fn(s.state)
	if !ok {
		return nil
	}

	s.dirty = true
	return s.emit(outFrame{kind: frame.Push, key: key, messageType: websocket.TextMessage, payload: msg})
}

func (w *Worker) Start() error {
	errCh := make(chan error, 2)

	go func() {
		errCh <- w.startGRPC()
	}()

	go func() {
		errCh <- w.startWS()
	}()

	snapshotCtx, stopSnapshots := context.WithCancel(context.Background())
	defer stopSnapshots()
	go w.StartSnapshots(snapshotCtx)
	go w.StartRegistration(snapshotCtx)

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
	w.drainOnce.Do(func() { close(w.draining) })
	defer w.unregister()

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
		resp, err := client.drain(w.id, reason, w.registerToken)
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

		time.Sleep(w.drainPoll)
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
	migratorpb.RegisterWorkerServiceServer(srv, newGRPCServer(w.id, w.sessions, w.onCreate, w.healthInterval, w))
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
	resumed := r.URL.Query().Get("resumed") == "1"
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

	if staged, ok := w.sessions.take(sessionID); ok {
		s.state = staged.state
		s.appliedSeq = staged.appliedSeq
		s.outbox = staged.outbox
	} else if w.onCreate != nil {
		s.state = w.onCreate(sessionID)
	}

	if err := w.sessions.add(s); err != nil {
		log.Printf("[worker-%s] rejected session %s: %v", w.id, sessionID, err)
		conn.Close()
		return
	}

	defer conn.Close()
	defer func() {
		w.sessions.remove(sessionID)
		if w.onEnd != nil {
			w.onEnd(sessionID, s.state)
		}
	}()

	log.Printf("[worker-%s] session %s connected", w.id, sessionID)

	s.mu.Lock()
	err = s.flushOutbox()
	s.mu.Unlock()
	if err != nil {
		log.Printf("[worker-%s] session %s: could not re-send unacked frames: %v", w.id, sessionID, err)
		return
	}

	if w.onReady != nil {
		w.onReady(Ready{SessionID: sessionID, State: s.state, Resumed: resumed})
	}

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

		kind, seq, payload, ok := frame.DecodeUp(msg)
		if !ok {
			log.Printf("[worker-%s] session %s: malformed frame, dropped", w.id, sessionID)
			continue
		}

		if kind == frame.Ack {
			reply, push, ok := frame.DecodeAck(payload)
			if !ok {
				log.Printf("[worker-%s] session %s: malformed ack, dropped", w.id, sessionID)
				continue
			}
			s.mu.Lock()
			s.outbox = trimOutbox(s.outbox, reply, push)
			s.mu.Unlock()
			continue
		}

		s.mu.Lock()
		if s.frozen {
			s.mu.Unlock()
			log.Printf("[worker-%s] session %s: frozen, left seq %d for the next worker", w.id, sessionID, seq)
			continue
		}
		if seq <= s.appliedSeq {
			s.mu.Unlock()
			log.Printf("[worker-%s] session %s: skipped duplicate seq %d", w.id, sessionID, seq)
			continue
		}
		if seq > s.appliedSeq+1 {
			s.mu.Unlock()
			log.Printf("[worker-%s] session %s: seq %d arrived ahead of %d, waiting for the resend", w.id, sessionID, seq, s.appliedSeq+1)
			continue
		}

		var response []byte
		if w.onMessage != nil {
			response = w.onMessage(sessionID, s.state, payload)
		} else {
			response = payload
		}
		s.appliedSeq = seq
		s.dirty = true
		err = s.emit(outFrame{kind: frame.Reply, key: seq, messageType: byte(messageType), payload: response})
		s.mu.Unlock()

		if err != nil {
			log.Printf("[worker-%s] write error: %v", w.id, err)
			return
		}
	}
}

func advertised(scheme, listenAddr, path string) string {
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return ""
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		if name, err := os.Hostname(); err == nil {
			host = name
		}
	}
	return scheme + net.JoinHostPort(host, port) + path
}
