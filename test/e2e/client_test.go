//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

const (
	tickEvery   = 50 * time.Millisecond
	replyWithin = 30 * time.Second
)

type stats struct {
	SessionsActive         uint64 `json:"sessions_active"`
	MigrationsSucceeded    uint64 `json:"migrations_succeeded"`
	MigrationsFailed       uint64 `json:"migrations_failed"`
	RecoveriesSucceeded    uint64 `json:"recoveries_succeeded"`
	RecoveriesFailed       uint64 `json:"recoveries_failed"`
	ResendGaps             uint64 `json:"resend_gaps"`
	DuplicatesDropped      uint64 `json:"duplicates_dropped"`
	WorkerSnapshotFailures uint64 `json:"worker_snapshot_failures"`
	WorkerSnapshotsWritten uint64 `json:"worker_snapshots_written"`
	WorkersTotal           int    `json:"workers_total"`
	WorkersHealthy         int    `json:"workers_healthy"`
}

type session struct {
	conn *websocket.Conn

	mu      sync.Mutex
	count   uint64
	worker  string
	failure string
}

func (s *session) view() (uint64, string, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count, s.worker, s.failure
}

func (s *session) fail(format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failure == "" {
		s.failure = fmt.Sprintf(format, args...)
	}
}

func (s *session) run(stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}

		s.conn.SetWriteDeadline(time.Now().Add(replyWithin))
		if err := s.conn.WriteMessage(websocket.TextMessage, []byte("tick")); err != nil {
			s.fail("write: %v", err)
			return
		}

		s.conn.SetReadDeadline(time.Now().Add(replyWithin))
		_, data, err := s.conn.ReadMessage()
		if err != nil {
			s.fail("read: %v", err)
			return
		}

		worker, count, ok := parseReply(string(data))
		if !ok {
			s.fail("unparsable reply %q", data)
			return
		}

		s.mu.Lock()
		if count != s.count+1 {
			s.failure = fmt.Sprintf("count went from %d to %d on %s: %s", s.count, count, worker, verdict(s.count, count))
			s.mu.Unlock()
			return
		}
		s.count = count
		s.worker = worker
		s.mu.Unlock()

		time.Sleep(tickEvery)
	}
}

func verdict(before, after uint64) string {
	if after <= before {
		return "a message was applied twice or state rolled back"
	}
	return "a message was lost"
}

func parseReply(raw string) (string, uint64, bool) {
	var worker string
	var count uint64
	for _, field := range strings.Fields(raw) {
		key, value, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		switch key {
		case "worker":
			worker = value
		case "count":
			n, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return "", 0, false
			}
			count = n
		}
	}
	return worker, count, worker != "" && count > 0
}

type fleet struct {
	t        *testing.T
	base     string
	sessions []*session
	stop     chan struct{}
	wg       sync.WaitGroup
}

func openFleet(t *testing.T, base string, n int) *fleet {
	t.Helper()

	f := &fleet{t: t, base: base, stop: make(chan struct{})}
	url := "ws" + strings.TrimPrefix(base, "http") + "/ws"

	for i := 0; i < n; i++ {
		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			t.Fatalf("session %d dial: %v", i, err)
		}
		s := &session{conn: conn}
		f.sessions = append(f.sessions, s)

		f.wg.Add(1)
		go func() {
			defer f.wg.Done()
			s.run(f.stop)
		}()
	}

	t.Cleanup(f.close)
	f.waitProgress("sessions start", 5)
	return f
}

func (f *fleet) close() {
	select {
	case <-f.stop:
		return
	default:
	}
	close(f.stop)
	f.wg.Wait()
	for _, s := range f.sessions {
		s.conn.Close()
	}
}

func (f *fleet) check(step string) {
	f.t.Helper()
	for i, s := range f.sessions {
		if _, _, failure := s.view(); failure != "" {
			f.t.Fatalf("after %s, session %d failed: %s", step, i, failure)
		}
	}
}

func (f *fleet) counts() []uint64 {
	out := make([]uint64, len(f.sessions))
	for i, s := range f.sessions {
		out[i], _, _ = s.view()
	}
	return out
}

func (f *fleet) waitProgress(step string, by uint64) {
	f.t.Helper()

	start := f.counts()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		f.check(step)
		done := true
		for i, c := range f.counts() {
			if c < start[i]+by {
				done = false
				break
			}
		}
		if done {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	f.t.Fatalf("after %s, not every session advanced by %d within 90s", step, by)
}

func (f *fleet) placement() map[string]int {
	out := map[string]int{}
	for _, s := range f.sessions {
		_, worker, _ := s.view()
		out[worker]++
	}
	return out
}

func (f *fleet) stats() stats {
	f.t.Helper()

	resp, err := http.Get(f.base + "/admin/stats")
	if err != nil {
		f.t.Fatalf("stats: %v", err)
	}
	defer resp.Body.Close()

	var s stats
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		f.t.Fatalf("decode stats: %v", err)
	}
	return s
}

func (f *fleet) waitStats(what string, ok func(stats) bool) stats {
	f.t.Helper()

	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		f.check(what)
		if s := f.stats(); ok(s) {
			return s
		}
		time.Sleep(500 * time.Millisecond)
	}
	f.t.Fatalf("timed out waiting for %s; last stats %+v", what, f.stats())
	return stats{}
}

func waitHTTP(t *testing.T, url string, within time.Duration) {
	t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if resp, err := http.Get(url); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("%s not reachable within %s", url, within)
}

func sh(t *testing.T, name string, args ...string) string {
	t.Helper()

	var out bytes.Buffer
	cmd := exec.Command(name, args...)
	cmd.Dir = repoRoot(t)
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out.String())
	}
	return out.String()
}

func repoRoot(t *testing.T) string {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSuffix(strings.TrimSuffix(wd, "/test/e2e"), `\test\e2e`)
}
