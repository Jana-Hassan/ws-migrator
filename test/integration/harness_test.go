package integration

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"

	"github.com/Jana-Hassan/ws-migrator/gateway"
	migratorpb "github.com/Jana-Hassan/ws-migrator/proto"
	"github.com/Jana-Hassan/ws-migrator/store"
	"github.com/Jana-Hassan/ws-migrator/worker"
)

const (
	testDrainTimeout     = 1 * time.Second
	waitTimeout          = 5 * time.Second
	ioTimeout            = 5 * time.Second
	testHealthInterval   = 50 * time.Millisecond
	slowHealthInterval   = 30 * time.Second
	testSnapshotInterval = 50 * time.Millisecond
	testRegisterInterval = 50 * time.Millisecond
	testPendingTTL       = 150 * time.Millisecond
)

var testHealthConfig = gateway.HealthConfig{
	Timeout:             400 * time.Millisecond,
	RecoverUpdates:      2,
	MinBackoff:          20 * time.Millisecond,
	MaxBackoff:          100 * time.Millisecond,
	ReconcileInterval:   20 * time.Millisecond,
	EvictAfterUnhealthy: 600 * time.Millisecond,
}

type counter struct {
	Count int
}

func (c *counter) Serialize() ([]byte, error) {
	return []byte(strconv.Itoa(c.Count)), nil
}

func (c *counter) Restore(data []byte) error {
	n, err := strconv.Atoi(string(data))
	if err != nil {
		return err
	}
	c.Count = n
	return nil
}

type failingState struct {
	counter
}

func (f *failingState) Serialize() ([]byte, error) {
	return nil, fmt.Errorf("serialize refused")
}

type testWorker struct {
	id     string
	worker *worker.Worker
	wsURL  string
	lis    net.Listener
	mu     sync.Mutex
	conns  []net.Conn
}

func (tw *testWorker) track(conn net.Conn) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	tw.conns = append(tw.conns, conn)
}

func (tw *testWorker) kill() {
	tw.lis.Close()

	tw.mu.Lock()
	defer tw.mu.Unlock()
	for _, conn := range tw.conns {
		conn.Close()
	}
	tw.conns = nil
}

type cluster struct {
	t           *testing.T
	gateway     *gateway.Server
	http        *httptest.Server
	pool        *gateway.Pool
	workers     []*testWorker
	gatewayAddr string
	redis       *miniredis.Miniredis
	store       store.Store

	token string

	endsMu sync.Mutex
	ends   []sessionEnd
}

type sessionEnd struct {
	workerID  string
	sessionID string
}

func (c *cluster) recordEnd(workerID, sessionID string) {
	c.endsMu.Lock()
	defer c.endsMu.Unlock()
	c.ends = append(c.ends, sessionEnd{workerID: workerID, sessionID: sessionID})
}

func (c *cluster) endsFor(sessionID string) []string {
	c.endsMu.Lock()
	defer c.endsMu.Unlock()

	var workers []string
	for _, e := range c.ends {
		if e.sessionID == sessionID {
			workers = append(workers, e.workerID)
		}
	}
	return workers
}

func (c *cluster) waitForSessionOff(sessionID, deadWorker string) string {
	c.t.Helper()

	var landed string
	waitFor(c.t, "session "+sessionID+" to move off "+deadWorker, func() bool {
		data, err := c.store.Load(context.Background(), sessionID)
		if err != nil || data == nil {
			return false
		}
		landed = data.WorkerID
		return landed != "" && landed != deadWorker
	})
	return landed
}

func (c *cluster) snapshot(sessionID string) (string, bool) {
	c.t.Helper()

	state, err := c.store.LoadSnapshot(context.Background(), sessionID)
	if err != nil || state == nil {
		return "", false
	}
	return string(state), true
}

func (c *cluster) dropSnapshot(sessionID string) {
	c.t.Helper()

	if err := c.store.DeleteSnapshot(context.Background(), sessionID); err != nil {
		c.t.Fatalf("drop snapshot: %v", err)
	}
}

type reply struct {
	worker    string
	sessionID string
	count     int
}

func newCluster(t *testing.T, workerCount int) *cluster {
	t.Helper()
	return newClusterWith(t, workerCount, func(sessionID string) worker.State { return &counter{} })
}

func newClusterWith(t *testing.T, workerCount int, onCreate func(string) worker.State) *cluster {
	t.Helper()

	redis := miniredis.RunT(t)
	return newClusterOn(t, workerCount, onCreate, store.NewRedis(redis.Addr()), redis)
}

func newClusterOn(t *testing.T, workerCount int, onCreate func(string) worker.State, backing store.Store, redis *miniredis.Miniredis) *cluster {
	t.Helper()

	pool := gateway.NewPool()
	gw := gateway.NewServer(pool, backing)

	gwLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("gateway grpc listen: %v", err)
	}
	go gw.StartGRPCOn(gwLis)
	t.Cleanup(func() { gwLis.Close() })

	httpSrv := httptest.NewServer(gw.Handler())
	t.Cleanup(httpSrv.Close)

	c := &cluster{t: t, gateway: gw, http: httpSrv, pool: pool, gatewayAddr: gwLis.Addr().String(), redis: redis, store: backing}

	for i := 1; i <= workerCount; i++ {
		c.addWorker(fmt.Sprintf("worker-%d", i), onCreate, testHealthInterval)
	}

	return c
}

func newEmptyCluster(t *testing.T) *cluster {
	t.Helper()

	redis := miniredis.RunT(t)
	onCreate := func(sessionID string) worker.State { return &counter{} }
	return newClusterOn(t, 0, onCreate, store.NewRedis(redis.Addr()), redis)
}

func (c *cluster) healthyInPool(id string) bool {
	c.t.Helper()

	w, ok := c.pool.Get(id)
	return ok && w.Healthy
}

func (c *cluster) startHealth() {
	c.t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	c.t.Cleanup(cancel)
	c.gateway.StartHealthMonitor(ctx, testHealthConfig)
}

func (c *cluster) healthy(id string) bool {
	c.t.Helper()

	w, ok := c.pool.Get(id)
	if !ok {
		c.t.Fatalf("worker %s not in pool", id)
	}
	return w.Healthy
}

func (c *cluster) addWorker(id string, onCreate func(string) worker.State, healthInterval time.Duration) *testWorker {
	c.t.Helper()
	return c.addWorkerOn(id, onCreate, healthInterval, c.store)
}

func (c *cluster) addWorkerWithoutStore(id string) *testWorker {
	c.t.Helper()
	return c.addWorkerOn(id, func(string) worker.State { return &counter{} }, testHealthInterval, nil)
}

func (c *cluster) addWorkerOn(id string, onCreate func(string) worker.State, healthInterval time.Duration, backing store.Store) *testWorker {
	c.t.Helper()
	return c.buildWorker(id, onCreate, healthInterval, backing, true)
}

func (c *cluster) addSelfRegisteringWorker(id string) *testWorker {
	c.t.Helper()
	return c.buildWorker(id, func(string) worker.State { return &counter{} }, testHealthInterval, c.store, false)
}

func (c *cluster) buildWorker(id string, onCreate func(string) worker.State, healthInterval time.Duration, backing store.Store, seed bool) *testWorker {
	c.t.Helper()

	grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		c.t.Fatalf("worker grpc listen: %v", err)
	}
	c.t.Cleanup(func() { grpcLis.Close() })

	wsLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		c.t.Fatalf("worker ws listen: %v", err)
	}
	wsURL := "ws://" + wsLis.Addr().String() + "/ws"

	w := worker.NewWorker(worker.Config{
		ID:               id,
		GatewayAddr:      c.gatewayAddr,
		DrainTimeout:     testDrainTimeout,
		HealthInterval:   healthInterval,
		SnapshotInterval: testSnapshotInterval,
		RegisterInterval: testRegisterInterval,
		RegisterToken:    c.token,
		AdvertiseWS:      wsURL,
		AdvertiseGRPC:    grpcLis.Addr().String(),
		PendingTTL:       testPendingTTL,
		Store:            backing,
	})

	w.OnSessionCreate(onCreate)
	w.OnSessionEnd(func(sessionID string, state worker.State) {
		c.recordEnd(id, sessionID)
	})
	w.OnMessage(func(sessionID string, state worker.State, msg []byte) []byte {
		n := 0
		switch s := state.(type) {
		case *counter:
			s.Count++
			n = s.Count
		case *failingState:
			s.Count++
			n = s.Count
		}
		return []byte(fmt.Sprintf("worker=%s session=%s count=%d", id, sessionID, n))
	})

	go w.StartGRPCOn(grpcLis)

	snapshotCtx, stopSnapshots := context.WithCancel(context.Background())
	c.t.Cleanup(stopSnapshots)
	go w.StartSnapshots(snapshotCtx)

	if !seed {
		go w.StartRegistration(snapshotCtx)
	}

	tw := &testWorker{
		id:     id,
		worker: w,
		wsURL:  wsURL,
		lis:    wsLis,
	}

	srv := &http.Server{
		Handler: w.Handler(),
		ConnState: func(conn net.Conn, state http.ConnState) {
			if state == http.StateHijacked {
				tw.track(conn)
			}
		},
	}

	go srv.Serve(wsLis)
	c.t.Cleanup(tw.kill)

	if seed {
		c.pool.Add(&gateway.WorkerInfo{
			ID:       id,
			WSAddr:   tw.wsURL,
			GRPCAddr: grpcLis.Addr().String(),
		})
	}

	c.workers = append(c.workers, tw)
	return tw
}

type fakeWorker struct {
	migratorpb.UnimplementedWorkerServiceServer
	id    string
	beats chan struct{}
	srv   *grpc.Server
}

func (f *fakeWorker) HealthStream(req *migratorpb.HealthStreamRequest, stream grpc.ServerStreamingServer[migratorpb.HealthUpdate]) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-f.beats:
			err := stream.Send(&migratorpb.HealthUpdate{
				WorkerId: f.id,
				Status:   migratorpb.WorkerStatus_WORKER_STATUS_SERVING,
				UnixMs:   time.Now().UnixMilli(),
			})
			if err != nil {
				return err
			}
		}
	}
}

func (f *fakeWorker) beat(t *testing.T) {
	t.Helper()

	select {
	case f.beats <- struct{}{}:
	case <-time.After(2 * time.Second):
		t.Fatalf("nothing consumed a heartbeat from %s: is it being monitored?", f.id)
	}
}

func (f *fakeWorker) stop() {
	f.srv.Stop()
}

func (c *cluster) addFakeWorker(id string) *fakeWorker {
	c.t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		c.t.Fatalf("fake worker listen: %v", err)
	}

	f := &fakeWorker{id: id, beats: make(chan struct{}), srv: grpc.NewServer()}
	migratorpb.RegisterWorkerServiceServer(f.srv, f)

	go f.srv.Serve(lis)
	c.t.Cleanup(f.srv.Stop)

	c.pool.Add(&gateway.WorkerInfo{
		ID:       id,
		WSAddr:   "ws://127.0.0.1:1/ws",
		GRPCAddr: lis.Addr().String(),
	})

	return f
}

func (c *cluster) otherWorker(id string) string {
	c.t.Helper()
	for _, w := range c.workers {
		if w.id != id {
			return w.id
		}
	}
	c.t.Fatalf("no worker other than %s", id)
	return ""
}

func (c *cluster) worker(id string) *testWorker {
	c.t.Helper()
	for _, w := range c.workers {
		if w.id == id {
			return w
		}
	}
	c.t.Fatalf("worker %s not in cluster", id)
	return nil
}

type testClient struct {
	t       *testing.T
	conn    *websocket.Conn
	last    int
	session string
}

func (c *cluster) connect() *testClient {
	c.t.Helper()

	url := "ws" + strings.TrimPrefix(c.http.URL, "http") + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		c.t.Fatalf("client dial: %v", err)
	}
	c.t.Cleanup(func() { conn.Close() })

	return &testClient{t: c.t, conn: conn}
}

func (tc *testClient) send(msg string) reply {
	tc.t.Helper()

	tc.conn.SetWriteDeadline(time.Now().Add(ioTimeout))
	if err := tc.conn.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
		tc.t.Fatalf("client write: %v", err)
	}

	tc.conn.SetReadDeadline(time.Now().Add(ioTimeout))
	_, data, err := tc.conn.ReadMessage()
	if err != nil {
		tc.t.Fatalf("client read: %v", err)
	}

	r := parseReply(tc.t, string(data))
	tc.session = r.sessionID
	return r
}

func (tc *testClient) sendExpectingNext(msg string) reply {
	tc.t.Helper()

	r := tc.send(msg)
	if r.count != tc.last+1 {
		tc.t.Fatalf("state lost: count %d, wanted %d (reply %+v)", r.count, tc.last+1, r)
	}
	tc.last = r.count
	return r
}

func parseReply(t *testing.T, raw string) reply {
	t.Helper()

	var r reply
	for _, field := range strings.Fields(raw) {
		key, value, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		switch key {
		case "worker":
			r.worker = value
		case "session":
			r.sessionID = value
		case "count":
			n, err := strconv.Atoi(value)
			if err != nil {
				t.Fatalf("bad count in reply %q: %v", raw, err)
			}
			r.count = n
		}
	}

	if r.worker == "" || r.sessionID == "" {
		t.Fatalf("unparsable reply %q", raw)
	}
	return r
}

func (c *cluster) migrate(sessionID, targetWorker string) (int, string) {
	c.t.Helper()

	url := fmt.Sprintf("%s/admin/migrate?session_id=%s&worker=%s", c.http.URL, sessionID, targetWorker)
	resp, err := http.Get(url)
	if err != nil {
		c.t.Fatalf("migrate request: %v", err)
	}
	defer resp.Body.Close()

	body := make([]byte, 256)
	n, _ := resp.Body.Read(body)
	return resp.StatusCode, strings.TrimSpace(string(body[:n]))
}

func (c *cluster) migrateOK(sessionID, targetWorker string) {
	c.t.Helper()

	if status, body := c.migrate(sessionID, targetWorker); status != http.StatusOK {
		c.t.Fatalf("migrate to %s: status %d (%s)", targetWorker, status, body)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func (tc *testClient) waitForWorker(workerID string) reply {
	tc.t.Helper()

	var r reply
	waitFor(tc.t, "session to land on "+workerID, func() bool {
		r = tc.sendExpectingNext("ping")
		return r.worker == workerID
	})
	return r
}
