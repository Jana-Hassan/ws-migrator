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

type mixedState struct {
	Replies uint64
	Pushes  uint64
}

func (m *mixedState) Serialize() ([]byte, error) {
	return []byte(fmt.Sprintf("%d,%d", m.Replies, m.Pushes)), nil
}

func (m *mixedState) Restore(data []byte) error {
	replies, pushes, ok := strings.Cut(string(data), ",")
	if !ok {
		return fmt.Errorf("bad mixed state %q", data)
	}

	r, err := strconv.ParseUint(replies, 10, 64)
	if err != nil {
		return err
	}
	p, err := strconv.ParseUint(pushes, 10, 64)
	if err != nil {
		return err
	}

	m.Replies, m.Pushes = r, p
	return nil
}

func newMixedCluster(t *testing.T, pushEvery time.Duration) *cluster {
	t.Helper()

	c := newEmptyCluster(t)
	c.configureWorker = func(id string, w *worker.Worker) {
		pumps := &mixedPumps{stops: map[string]chan struct{}{}}

		w.OnSessionCreate(func(string) worker.State { return &mixedState{} })

		w.OnMessage(func(sessionID string, state worker.State, msg []byte) []byte {
			st := state.(*mixedState)
			st.Replies++
			return []byte(fmt.Sprintf("reply=%d", st.Replies))
		})

		w.OnSessionReady(func(r worker.Ready) {
			pumps.start(w, r.SessionID, pushEvery)
		})

		w.OnSessionEnd(func(sessionID string, state worker.State) {
			pumps.stop(sessionID)
		})
	}
	return c
}

type mixedPumps struct {
	mu    sync.Mutex
	stops map[string]chan struct{}
}

func (p *mixedPumps) start(w *worker.Worker, sessionID string, every time.Duration) {
	stop := make(chan struct{})

	p.mu.Lock()
	if old, ok := p.stops[sessionID]; ok {
		close(old)
	}
	p.stops[sessionID] = stop
	p.mu.Unlock()

	go func() {
		ticker := time.NewTicker(every)
		defer ticker.Stop()

		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
			}

			err := w.Push(sessionID, func(state worker.State) (uint64, []byte, bool) {
				st := state.(*mixedState)
				st.Pushes++
				return st.Pushes, []byte(fmt.Sprintf("push=%d", st.Pushes)), true
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

func (p *mixedPumps) stop(sessionID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if stop, ok := p.stops[sessionID]; ok {
		close(stop)
		delete(p.stops, sessionID)
	}
}

type stream struct {
	last    uint64
	repeats int
	gaps    int
	lost    uint64
}

func (s *stream) observe(n uint64) {
	switch {
	case n == s.last+1:
		s.last = n
	case n <= s.last:
		s.repeats++
	default:
		s.gaps++
		s.lost += n - s.last - 1
		s.last = n
	}
}

type mixedClient struct {
	conn *websocket.Conn

	mu      sync.Mutex
	sent    uint64
	replies stream
	pushes  stream
	failure string
}

func (c *cluster) connectMixedClient() *mixedClient {
	c.t.Helper()

	conn, _, err := dialGateway(c)
	if err != nil {
		c.t.Fatalf("mixed client dial: %v", err)
	}
	c.t.Cleanup(func() { conn.Close() })

	mc := &mixedClient{conn: conn}
	go mc.read()
	return mc
}

func (mc *mixedClient) read() {
	for {
		mc.conn.SetReadDeadline(time.Now().Add(pushClientLifetime))
		_, data, err := mc.conn.ReadMessage()
		if err != nil {
			mc.mu.Lock()
			if mc.failure == "" {
				mc.failure = "read: " + err.Error()
			}
			mc.mu.Unlock()
			return
		}

		kind, value, ok := strings.Cut(string(data), "=")
		n, perr := strconv.ParseUint(value, 10, 64)
		if !ok || perr != nil {
			mc.mu.Lock()
			mc.failure = fmt.Sprintf("unparsable frame %q", data)
			mc.mu.Unlock()
			return
		}

		mc.mu.Lock()
		switch kind {
		case "reply":
			mc.replies.observe(n)
		case "push":
			mc.pushes.observe(n)
		default:
			mc.failure = fmt.Sprintf("unknown frame %q", data)
		}
		mc.mu.Unlock()
	}
}

func (mc *mixedClient) write(stop <-chan struct{}, every time.Duration) {
	for {
		select {
		case <-stop:
			return
		default:
		}

		mc.conn.SetWriteDeadline(time.Now().Add(ioTimeout))
		if err := mc.conn.WriteMessage(websocket.TextMessage, []byte("req")); err != nil {
			return
		}

		mc.mu.Lock()
		mc.sent++
		mc.mu.Unlock()

		time.Sleep(every)
	}
}

func (mc *mixedClient) view() (sent uint64, replies, pushes stream, failure string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return mc.sent, mc.replies, mc.pushes, mc.failure
}

func TestMixedTrafficSurvivesCrash(t *testing.T) {
	c := newMixedCluster(t, 5*time.Millisecond)
	c.startHealth()
	c.addScaleWorker("worker-1")

	clients := make([]*mixedClient, 5)
	for i := range clients {
		clients[i] = c.connectMixedClient()
	}

	stop := make(chan struct{})
	var writers sync.WaitGroup
	for _, mc := range clients {
		writers.Add(1)
		go func(mc *mixedClient) {
			defer writers.Done()
			mc.write(stop, 3*time.Millisecond)
		}(mc)
	}

	time.Sleep(400 * time.Millisecond)
	c.addScaleWorker("worker-2")

	before := make([]stream, len(clients))
	beforePushes := make([]stream, len(clients))
	for i, mc := range clients {
		_, before[i], beforePushes[i], _ = mc.view()
	}

	c.worker("worker-1").kill()

	for _, id := range c.sessionIDs() {
		c.waitForSessionOff(id, "worker-1")
	}

	time.Sleep(400 * time.Millisecond)
	close(stop)
	writers.Wait()

	waitFor(t, "every request answered", func() bool {
		for _, mc := range clients {
			sent, replies, _, failure := mc.view()
			if failure != "" || replies.last < sent {
				return false
			}
		}
		return true
	})

	stats := c.stats()
	t.Logf("recoveries: %d, duplicates dropped by gateway: %d, resends: %d",
		stats.RecoveriesSucceeded, stats.DuplicatesDropped, stats.ResendsSent)

	for i, mc := range clients {
		sent, replies, pushes, failure := mc.view()
		t.Logf("client %d: %d requests, replies up to %d (%d repeats, %d lost), pushes up to %d (%d repeats, %d lost)",
			i, sent, replies.last, replies.repeats, replies.lost, pushes.last, pushes.repeats, pushes.lost)

		if failure != "" {
			t.Errorf("client %d: %s", i, failure)
		}
		if replies.repeats > 0 || replies.lost > 0 {
			t.Errorf("client %d: replies had %d repeats and %d lost", i, replies.repeats, replies.lost)
		}
		if pushes.repeats > 0 || pushes.lost > 0 {
			t.Errorf("client %d: pushes had %d repeats and %d lost", i, pushes.repeats, pushes.lost)
		}
		if replies.last != sent {
			t.Errorf("client %d: sent %d requests but replies stop at %d", i, sent, replies.last)
		}
		if replies.last <= before[i].last {
			t.Errorf("client %d: replies did not advance after the crash", i)
		}
		if pushes.last <= beforePushes[i].last {
			t.Errorf("client %d: pushes did not advance after the crash", i)
		}
	}

	if stats.RecoveriesSucceeded != uint64(len(clients)) {
		t.Errorf("%d recoveries, wanted %d", stats.RecoveriesSucceeded, len(clients))
	}
}
