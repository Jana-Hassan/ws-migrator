package integration

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gorilla/websocket"

	"github.com/Jana-Hassan/ws-migrator/gateway"
	"github.com/Jana-Hassan/ws-migrator/store"
	"github.com/Jana-Hassan/ws-migrator/worker"
)

const (
	testDrainTimeout = 1 * time.Second
	waitTimeout      = 5 * time.Second
	ioTimeout        = 5 * time.Second
)

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
	ws     *httptest.Server
}

type cluster struct {
	t       *testing.T
	gateway *gateway.Server
	http    *httptest.Server
	pool    *gateway.Pool
	workers []*testWorker
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
	pool := gateway.NewPool()
	gw := gateway.NewServer(pool, store.New(redis.Addr()))

	gwLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("gateway grpc listen: %v", err)
	}
	go gw.StartGRPCOn(gwLis)
	t.Cleanup(func() { gwLis.Close() })

	httpSrv := httptest.NewServer(gw.Handler())
	t.Cleanup(httpSrv.Close)

	c := &cluster{t: t, gateway: gw, http: httpSrv, pool: pool}

	for i := 1; i <= workerCount; i++ {
		c.addWorker(fmt.Sprintf("worker-%d", i), gwLis.Addr().String(), onCreate)
	}

	return c
}

func (c *cluster) addWorker(id, gatewayAddr string, onCreate func(string) worker.State) *testWorker {
	c.t.Helper()

	grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		c.t.Fatalf("worker grpc listen: %v", err)
	}
	c.t.Cleanup(func() { grpcLis.Close() })

	w := worker.NewWorker(worker.Config{
		ID:           id,
		GatewayAddr:  gatewayAddr,
		DrainTimeout: testDrainTimeout,
	})

	w.OnSessionCreate(onCreate)
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

	ws := httptest.NewServer(w.Handler())
	c.t.Cleanup(ws.Close)

	c.pool.Add(&gateway.WorkerInfo{
		ID:       id,
		WSAddr:   "ws" + strings.TrimPrefix(ws.URL, "http") + "/ws",
		GRPCAddr: grpcLis.Addr().String(),
	})

	tw := &testWorker{id: id, worker: w, ws: ws}
	c.workers = append(c.workers, tw)
	return tw
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
