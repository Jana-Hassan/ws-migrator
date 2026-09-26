package integration

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Jana-Hassan/ws-migrator/worker"
)

const pushClientLifetime = 60 * time.Second

type pushState struct {
	n uint64
}

func (p *pushState) Serialize() ([]byte, error) {
	return []byte(strconv.FormatUint(p.n, 10)), nil
}

func (p *pushState) Restore(data []byte) error {
	n, err := strconv.ParseUint(string(data), 10, 64)
	if err != nil {
		return err
	}
	p.n = n
	return nil
}

type pushPumps struct {
	mu    sync.Mutex
	stops map[string]chan struct{}
}

func (p *pushPumps) start(w *worker.Worker, sessionID string, interval time.Duration) {
	stop := make(chan struct{})

	p.mu.Lock()
	if p.stops == nil {
		p.stops = map[string]chan struct{}{}
	}
	if old, ok := p.stops[sessionID]; ok {
		close(old)
	}
	p.stops[sessionID] = stop
	p.mu.Unlock()

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
			}

			err := w.Push(sessionID, func(state worker.State) (uint64, []byte, bool) {
				ps := state.(*pushState)
				ps.n++
				return ps.n, []byte(fmt.Sprintf("push=%d", ps.n)), true
			})
			if errors.Is(err, worker.ErrFrozen) {
				continue
			}
			if err != nil {
				return
			}
		}
	}()
}

func (p *pushPumps) stop(sessionID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if stop, ok := p.stops[sessionID]; ok {
		close(stop)
		delete(p.stops, sessionID)
	}
}

func newPushCluster(t *testing.T, workers int, interval time.Duration) *cluster {
	t.Helper()

	c := newEmptyCluster(t)
	c.drainTimeout = 10 * time.Second

	c.configureWorker = func(id string, w *worker.Worker) {
		pumps := &pushPumps{}

		w.OnSessionCreate(func(sessionID string) worker.State {
			return &pushState{}
		})

		w.OnMessage(func(sessionID string, state worker.State, msg []byte) []byte {
			return []byte("ack")
		})

		w.OnSessionReady(func(r worker.Ready) {
			pumps.start(w, r.SessionID, interval)
		})

		w.OnSessionEnd(func(sessionID string, state worker.State) {
			pumps.stop(sessionID)
		})
	}

	for i := 1; i <= workers; i++ {
		c.addScaleWorker(fmt.Sprintf("worker-%d", i))
	}
	return c
}

type pushClient struct {
	t    *testing.T
	conn *websocket.Conn

	mu      sync.Mutex
	lastSeq uint64
	pushes  int
	gaps    int
	lost    uint64
	repeats int
	failure string
}

func (c *cluster) connectPushClient() *pushClient {
	c.t.Helper()

	conn, _, err := dialGateway(c)
	if err != nil {
		c.t.Fatalf("push client dial: %v", err)
	}
	c.t.Cleanup(func() { conn.Close() })

	pc := &pushClient{t: c.t, conn: conn}

	conn.SetWriteDeadline(time.Now().Add(ioTimeout))
	if err := conn.WriteMessage(websocket.TextMessage, []byte("start")); err != nil {
		c.t.Fatalf("push client start: %v", err)
	}

	go pc.read()
	return pc
}

func (pc *pushClient) read() {
	for {
		pc.conn.SetReadDeadline(time.Now().Add(pushClientLifetime))
		_, data, err := pc.conn.ReadMessage()
		if err != nil {
			pc.mu.Lock()
			if pc.failure == "" {
				pc.failure = err.Error()
			}
			pc.mu.Unlock()
			return
		}

		body := string(data)
		if !strings.HasPrefix(body, "push=") {
			continue
		}

		seq, err := strconv.ParseUint(strings.TrimPrefix(body, "push="), 10, 64)
		if err != nil {
			pc.mu.Lock()
			pc.failure = "bad push " + body
			pc.mu.Unlock()
			return
		}

		pc.mu.Lock()
		pc.pushes++
		switch {
		case seq == pc.lastSeq+1:
			pc.lastSeq = seq
		case seq <= pc.lastSeq:
			pc.repeats++
		default:
			pc.gaps++
			pc.lost += seq - pc.lastSeq - 1
			pc.lastSeq = seq
		}
		pc.mu.Unlock()
	}
}

func (pc *pushClient) snapshot() (pushes int, gaps int, lost uint64, failure string) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.pushes, pc.gaps, pc.lost, pc.failure
}

func (pc *pushClient) repeated() int {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.repeats
}

func totalRepeats(clients []*pushClient) (repeats, affected int) {
	for _, pc := range clients {
		if n := pc.repeated(); n > 0 {
			repeats += n
			affected++
		}
	}
	return repeats, affected
}

func TestPushStreamSurvivesDrain(t *testing.T) {

	c := newPushCluster(t, 3, 10*time.Millisecond)

	clients := make([]*pushClient, 10)
	for i := range clients {
		clients[i] = c.connectPushClient()
	}

	time.Sleep(300 * time.Millisecond)
	before := totalPushes(clients)

	victim := c.workers[0]
	c.addScaleWorker("worker-replacement")
	victim.worker.Drain("push test")

	time.Sleep(200 * time.Millisecond)

	afterDrain := make([]int, len(clients))
	for i, pc := range clients {
		afterDrain[i], _, _, _ = pc.snapshot()
	}

	time.Sleep(300 * time.Millisecond)

	delivered, gapped, stalled, lost := 0, 0, 0, uint64(0)
	for i, pc := range clients {
		pushes, gaps, clientLost, failure := pc.snapshot()
		if failure != "" {
			t.Fatalf("client %d: %s", i, failure)
		}
		if pushes <= afterDrain[i] {
			stalled++
			t.Logf("client %d: push stream stopped after the drain (%d pushes, none since)", i, pushes)
		}
		delivered += pushes
		lost += clientLost
		if gaps > 0 {
			gapped++
		}
	}

	t.Logf("drain: %d pushes delivered (%d before the drain), %d lost, %d of %d clients saw a gap, %d stopped receiving",
		delivered, before, lost, gapped, len(clients), stalled)

	if stalled > 0 {
		t.Errorf("%d of %d push streams stopped after the drain", stalled, len(clients))
	}

	if repeats, affected := totalRepeats(clients); repeats > 0 {
		t.Errorf("%d repeated pushes across %d clients after a drain", repeats, affected)
	}
}

func TestPushStreamSurvivesCrash(t *testing.T) {

	c := newPushCluster(t, 3, 10*time.Millisecond)
	c.startHealth()

	clients := make([]*pushClient, 10)
	for i := range clients {
		clients[i] = c.connectPushClient()
	}

	time.Sleep(300 * time.Millisecond)

	counts := make([]int, len(clients))
	for i, pc := range clients {
		counts[i], _, _, _ = pc.snapshot()
	}

	c.workers[0].kill()
	time.Sleep(700 * time.Millisecond)

	resumed, lost := 0, uint64(0)
	for i, pc := range clients {
		pushes, _, clientLost, failure := pc.snapshot()
		if failure != "" {
			t.Fatalf("client %d: %s", i, failure)
		}
		lost += clientLost
		if pushes > counts[i] {
			resumed++
		}
	}

	repeats, affected := totalRepeats(clients)
	t.Logf("crash: %d of %d clients resumed pushes, %d pushes lost, %d repeated across %d clients",
		resumed, len(clients), lost, repeats, affected)

	if resumed != len(clients) {
		t.Errorf("%d of %d push streams stopped after the crash", len(clients)-resumed, len(clients))
	}

	if repeats, affected := totalRepeats(clients); repeats > 0 {
		t.Errorf("%d repeated pushes across %d clients after a crash", repeats, affected)
	}
}

func totalPushes(clients []*pushClient) int {
	total := 0
	for _, pc := range clients {
		pushes, _, _, _ := pc.snapshot()
		total += pushes
	}
	return total
}
