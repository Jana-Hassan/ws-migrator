package integration

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Jana-Hassan/ws-migrator/worker"
)

type pipelineClient struct {
	conn *websocket.Conn

	mu       sync.Mutex
	sent     int
	received int
	lastSeq  uint64
	failure  string
}

func (c *cluster) connectPipelineClient() *pipelineClient {
	c.t.Helper()

	conn, _, err := dialGateway(c)
	if err != nil {
		c.t.Fatalf("pipeline client dial: %v", err)
	}
	c.t.Cleanup(func() { conn.Close() })

	return &pipelineClient{conn: conn}
}

func (pc *pipelineClient) fail(format string, args ...any) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.failure == "" {
		pc.failure = fmt.Sprintf(format, args...)
	}
}

func (pc *pipelineClient) write(stop <-chan struct{}, rate time.Duration) {
	for {
		select {
		case <-stop:
			return
		default:
		}

		pc.conn.SetWriteDeadline(time.Now().Add(ioTimeout))
		if err := pc.conn.WriteMessage(websocket.TextMessage, []byte("tick")); err != nil {
			pc.fail("write: %v", err)
			return
		}

		pc.mu.Lock()
		pc.sent++
		pc.mu.Unlock()

		time.Sleep(rate)
	}
}

func (pc *pipelineClient) read() {
	for {
		pc.conn.SetReadDeadline(time.Now().Add(pushClientLifetime))
		_, data, err := pc.conn.ReadMessage()
		if err != nil {
			pc.fail("read: %v", err)
			return
		}

		_, _, count, ok := parseLoadReply(string(data))
		if !ok {
			pc.fail("unparsable reply %q", data)
			return
		}

		pc.mu.Lock()
		pc.received++
		if count != pc.lastSeq+1 {
			pc.failure = fmt.Sprintf("reply %d arrived after %d: %s", count, pc.lastSeq, verdict(count, pc.lastSeq))
			pc.mu.Unlock()
			return
		}
		pc.lastSeq = count
		pc.mu.Unlock()
	}
}

func verdict(count, last uint64) string {
	if count <= last {
		return "message applied twice"
	}
	return "message lost"
}

func (pc *pipelineClient) state() (sent, received int, failure string) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.sent, pc.received, pc.failure
}

func awaitAllReplies(t *testing.T, clients []*pipelineClient) {
	t.Helper()

	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		settled := true
		for _, pc := range clients {
			sent, received, failure := pc.state()
			if failure != "" {
				t.Fatalf("client failed while settling: %s", failure)
			}
			if received != sent {
				settled = false
				break
			}
		}
		if settled {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	for i, pc := range clients {
		if sent, received, _ := pc.state(); received != sent {
			t.Errorf("client %d: sent %d, got %d replies: %d messages were never applied", i, sent, received, sent-received)
		}
	}
	t.FailNow()
}

func newSlowCluster(t *testing.T, workers int, work time.Duration) *cluster {
	t.Helper()

	c := newEmptyCluster(t)
	c.drainTimeout = 30 * time.Second
	c.snapshotInterval = 500 * time.Millisecond
	c.configureWorker = func(id string, w *worker.Worker) {
		w.OnMessage(func(sessionID string, state worker.State, msg []byte) []byte {
			time.Sleep(work)
			ctr := state.(*counter)
			ctr.Count++
			return []byte(fmt.Sprintf("worker=%s session=%s count=%d", id, sessionID, ctr.Count))
		})
	}

	for i := 1; i <= workers; i++ {
		c.addScaleWorker(fmt.Sprintf("worker-%d", i))
	}
	return c
}

func TestPipelinedUplinkSurvivesRollingDrains(t *testing.T) {
	c := newScaleCluster(t, 6)
	c.startHealth()

	clients := make([]*pipelineClient, 40)
	for i := range clients {
		clients[i] = c.connectPipelineClient()
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for _, pc := range clients {
		wg.Add(2)
		go func(pc *pipelineClient) {
			defer wg.Done()
			pc.write(stop, 2*time.Millisecond)
		}(pc)
		go func(pc *pipelineClient) {
			defer wg.Done()
			pc.read()
		}(pc)
	}

	time.Sleep(200 * time.Millisecond)

	rolling := append([]*testWorker(nil), c.workers...)
	for i, tw := range rolling {
		c.addScaleWorker(fmt.Sprintf("replacement-%d", i))
		tw.worker.Drain("pipeline test")

		for j, pc := range clients {
			if _, _, failure := pc.state(); failure != "" {
				t.Fatalf("client %d after draining %s: %s", j, tw.id, failure)
			}
		}
	}

	close(stop)
	awaitAllReplies(t, clients)

	stats := c.stats()
	totalSent, totalReceived := 0, 0
	for i, pc := range clients {
		sent, received, failure := pc.state()
		if failure != "" {
			t.Fatalf("client %d: %s", i, failure)
		}
		totalSent += sent
		totalReceived += received
	}

	t.Logf("pipelined: %d sent, %d replies, %d resends, %d resend gaps, %d migrations",
		totalSent, totalReceived, stats.ResendsSent, stats.ResendGaps, stats.MigrationsSucceeded)

	if stats.ResendsSent == 0 {
		t.Log("no resends happened: this run did not exercise the in-flight window")
	}

	for _, pc := range clients {
		pc.conn.Close()
	}
	wg.Wait()
}

func TestPipelinedUplinkToSlowWorkerIsAppliedOnce(t *testing.T) {

	c := newSlowCluster(t, 3, 3*time.Millisecond)

	clients := make([]*pipelineClient, 10)
	for i := range clients {
		clients[i] = c.connectPipelineClient()
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for _, pc := range clients {
		wg.Add(2)
		go func(pc *pipelineClient) {
			defer wg.Done()
			pc.write(stop, time.Millisecond)
		}(pc)
		go func(pc *pipelineClient) {
			defer wg.Done()
			pc.read()
		}(pc)
	}

	time.Sleep(200 * time.Millisecond)

	victim := c.workers[0]
	c.addScaleWorker("replacement")
	victim.worker.Drain("slow worker test")

	time.Sleep(300 * time.Millisecond)
	close(stop)
	awaitAllReplies(t, clients)

	stats := c.stats()
	t.Logf("slow worker: %d resends, %d migrations", stats.ResendsSent, stats.MigrationsSucceeded)

	for i, pc := range clients {
		if _, _, failure := pc.state(); failure != "" && !strings.Contains(failure, "read:") {
			t.Errorf("client %d: %s", i, failure)
		}
	}

	for _, pc := range clients {
		pc.conn.Close()
	}
	wg.Wait()
}

func TestFailedMigrationAfterFreezeLosesNothing(t *testing.T) {
	c := newSlowCluster(t, 1, 3*time.Millisecond)

	clients := make([]*pipelineClient, 10)
	for i := range clients {
		clients[i] = c.connectPipelineClient()
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for _, pc := range clients {
		wg.Add(2)
		go func(pc *pipelineClient) {
			defer wg.Done()
			pc.write(stop, time.Millisecond)
		}(pc)
		go func(pc *pipelineClient) {
			defer wg.Done()
			pc.read()
		}(pc)
	}

	waitFor(t, "every client has a session", func() bool {
		return int(c.stats().SessionsActive) == len(clients)
	})
	time.Sleep(100 * time.Millisecond)

	c.addRestoreRejectingWorker("broken")

	for _, id := range c.sessionIDs() {
		c.migrate(id, "broken")
	}

	waitFor(t, "every migration to the broken worker failed", func() bool {
		return int(c.stats().MigrationsFailed) >= len(clients)
	})

	time.Sleep(200 * time.Millisecond)
	close(stop)
	awaitAllReplies(t, clients)

	stats := c.stats()
	t.Logf("failed migrations: %d, resends: %d", stats.MigrationsFailed, stats.ResendsSent)

	if stats.MigrationsSucceeded != 0 {
		t.Fatalf("%d migrations succeeded onto a worker that rejects every restore", stats.MigrationsSucceeded)
	}

	for _, pc := range clients {
		pc.conn.Close()
	}
	wg.Wait()
}
