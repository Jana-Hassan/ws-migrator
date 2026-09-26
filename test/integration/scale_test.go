package integration

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

const scaleIOTimeout = 20 * time.Second

func envInt(key string, fallback int) int {
	if raw := os.Getenv(key); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

type loadClient struct {
	conn    *websocket.Conn
	session atomic.Value
	worker  atomic.Value
	applied atomic.Uint64
	maxGap  atomic.Int64
	failure atomic.Value
}

func (lc *loadClient) fail(format string, args ...any) {
	if lc.failure.Load() == nil {
		lc.failure.Store(fmt.Sprintf(format, args...))
	}
}

func (lc *loadClient) run(stop <-chan struct{}, rate time.Duration) {
	expected := uint64(0)
	last := time.Now()

	for {
		select {
		case <-stop:
			return
		default:
		}

		lc.conn.SetWriteDeadline(time.Now().Add(scaleIOTimeout))
		if err := lc.conn.WriteMessage(websocket.TextMessage, []byte("tick")); err != nil {
			lc.fail("write failed: %v", err)
			return
		}

		lc.conn.SetReadDeadline(time.Now().Add(scaleIOTimeout))
		_, data, err := lc.conn.ReadMessage()
		if err != nil {
			lc.fail("read failed after %d messages: %v", expected, err)
			return
		}

		if gap := time.Since(last).Milliseconds(); gap > lc.maxGap.Load() {
			lc.maxGap.Store(gap)
		}
		last = time.Now()

		worker, session, count, ok := parseLoadReply(string(data))
		if !ok {
			lc.fail("unparsable reply %q", data)
			return
		}

		expected++
		if count != expected {
			lc.fail("count is %d after %d messages, wanted %d (on %s)", count, expected, expected, worker)
			return
		}

		lc.worker.Store(worker)
		lc.session.Store(session)
		lc.applied.Store(expected)

		time.Sleep(rate)
	}
}

func parseLoadReply(raw string) (worker, session string, count uint64, ok bool) {
	for _, field := range strings.Fields(raw) {
		key, value, found := strings.Cut(field, "=")
		if !found {
			continue
		}
		switch key {
		case "worker":
			worker = value
		case "session":
			session = value
		case "count":
			n, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return "", "", 0, false
			}
			count = n
		}
	}

	return worker, session, count, worker != "" && session != "" && count > 0
}

func TestSustainedLoadAcrossRollingRestarts(t *testing.T) {
	if os.Getenv("LOAD_SESSIONS") == "" {
		t.Skip("set LOAD_SESSIONS (e.g. 200) to run the sustained-scale test")
	}

	sessions := envInt("LOAD_SESSIONS", 200)
	cycles := envInt("LOAD_CYCLES", 1)
	rate := time.Duration(envInt("LOAD_RATE_MS", 20)) * time.Millisecond
	workers := envInt("LOAD_WORKERS", 8)

	c := newScaleCluster(t, workers)
	c.startHealth()

	baseline := countGoroutines(readClientFrame)

	clients := make([]*loadClient, sessions)
	for i := range clients {
		clients[i] = c.connectLoadClient()
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for _, lc := range clients {
		wg.Add(1)
		go func(lc *loadClient) {
			defer wg.Done()
			lc.run(stop, rate)
		}(lc)
	}

	waitFor(t, "all sessions serving", func() bool {
		return int(c.stats().SessionsActive) == sessions
	})

	migrationsBefore := c.stats().MigrationsSucceeded
	var drains []time.Duration

	for cycle := 1; cycle <= cycles; cycle++ {
		rolling := append([]*testWorker(nil), c.workers...)

		for _, tw := range rolling {
			if _, live := c.pool.Get(tw.id); !live {
				continue
			}

			replacement := c.addScaleWorker(fmt.Sprintf("%s-c%d", tw.id, cycle))
			waitFor(t, "replacement "+replacement.id+" registered", func() bool {
				_, ok := c.pool.Get(replacement.id)
				return ok
			})

			before := c.stats().MigrationsSucceeded
			start := time.Now()
			tw.worker.Drain("scale test")
			elapsed := time.Since(start)

			moved := c.stats().MigrationsSucceeded - before
			if moved > 0 {
				drains = append(drains, elapsed)
				t.Logf("cycle %d: %s drained %d sessions in %s", cycle, tw.id, moved, elapsed.Round(time.Millisecond))
			}

			checkLoadClients(t, clients)
			c.reportDistribution(fmt.Sprintf("cycle %d after draining %s", cycle, tw.id), c.storeDistribution(clients))
		}
	}

	close(stop)
	wg.Wait()

	checkLoadClients(t, clients)
	stats := c.stats()

	if stats.MigrationsFailed != 0 {
		t.Errorf("%d migrations failed", stats.MigrationsFailed)
	}
	if stats.ResendGaps != 0 {
		t.Errorf("%d resend gaps: the 1024-message ring overflowed", stats.ResendGaps)
	}
	if int(stats.SessionsActive) != sessions {
		t.Errorf("%d sessions active at the end, wanted %d", stats.SessionsActive, sessions)
	}

	counts := c.storeDistribution(clients)
	ideal := sessions / len(counts)
	worst, highest := maxLoad(counts)
	if highest > ideal*2 {
		t.Errorf("%s holds %d sessions, more than twice the even share of %d", worst, highest, ideal)
	}

	reportScale(t, clients, stats, drains, stats.MigrationsSucceeded-migrationsBefore)

	for _, lc := range clients {
		lc.conn.Close()
	}
	waitForGoroutines(t, readClientFrame, baseline)
}

func checkLoadClients(t *testing.T, clients []*loadClient) {
	t.Helper()

	for i, lc := range clients {
		if msg := lc.failure.Load(); msg != nil {
			t.Fatalf("client %d: %s", i, msg)
		}
	}
}

func reportScale(t *testing.T, clients []*loadClient, stats gatewayStats, drains []time.Duration, migrations uint64) {
	t.Helper()

	var messages, worstGap int64
	for _, lc := range clients {
		messages += int64(lc.applied.Load())
		if gap := lc.maxGap.Load(); gap > worstGap {
			worstGap = gap
		}
	}

	sort.Slice(drains, func(i, j int) bool { return drains[i] < drains[j] })

	line := func(label string, value any) {
		t.Logf("  %-26s %v", label, value)
	}

	t.Log("scale run summary")
	line("peak concurrent sessions", len(clients))
	line("migrations survived", migrations)
	line("messages acked by clients", messages)
	line("messages forwarded (gateway)", stats.MessagesForwarded)
	line("resend gaps", stats.ResendGaps)
	line("resends sent", stats.ResendsSent)
	line("migrations failed", stats.MigrationsFailed)
	line("recoveries attempted", stats.RecoveriesAttempted)
	line("worst client stall", fmt.Sprintf("%dms", worstGap))

	if len(drains) > 0 {
		line("drain min", drains[0].Round(time.Millisecond))
		line("drain median", drains[len(drains)/2].Round(time.Millisecond))
		line("drain max", drains[len(drains)-1].Round(time.Millisecond))
	}
}
