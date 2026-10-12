//go:build e2e

package e2e

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

const composeGateway = "http://localhost:18080"

func compose(t *testing.T, args ...string) string {
	t.Helper()
	return sh(t, "docker", append([]string{"compose", "-p", "wsm-e2e", "-f", "test/e2e/compose.yml"}, args...)...)
}

func gatewayStats(t *testing.T, base string) stats {
	t.Helper()

	resp, err := http.Get(base + "/admin/stats")
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	defer resp.Body.Close()

	var s stats
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	return s
}

func waitWorkers(t *testing.T, base string, want int) {
	t.Helper()

	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if s := gatewayStats(t, base); s.WorkersTotal == want && s.WorkersHealthy == want {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("expected %d healthy workers; last stats %+v", want, gatewayStats(t, base))
}

func TestCompose(t *testing.T) {
	compose(t, "down", "-v", "--remove-orphans")
	compose(t, "up", "-d", "--build")
	t.Cleanup(func() {
		if t.Failed() {
			t.Log(compose(t, "logs", "--tail", "40", "gateway"))
			t.Log(compose(t, "logs", "--tail", "40", "worker-3"))
			t.Log(compose(t, "ps"))
		}
		compose(t, "down", "-v", "--remove-orphans")
	})

	waitHTTP(t, composeGateway+"/health", 2*time.Minute)
	waitWorkers(t, composeGateway, 3)

	f := openFleet(t, composeGateway, 20)
	t.Logf("start: %v", f.placement())

	t.Run("graceful drain over SIGTERM", func(t *testing.T) {
		compose(t, "stop", "worker-1")
		f.waitProgress("draining worker-1", 20)

		if n := f.placement()["worker-1"]; n != 0 {
			t.Fatalf("%d sessions still on the drained worker", n)
		}
		s := f.waitStats("worker-1 to unregister", func(s stats) bool { return s.WorkersTotal == 2 })
		if s.MigrationsSucceeded == 0 {
			t.Fatal("the drain moved no sessions")
		}
		t.Logf("after drain: %v, %d migrations", f.placement(), s.MigrationsSucceeded)
	})

	compose(t, "start", "worker-1")
	waitWorkers(t, composeGateway, 3)

	t.Run("hard kill", func(t *testing.T) {
		victim := "worker-2"
		onVictim := f.placement()[victim]
		before := f.stats().RecoveriesSucceeded

		compose(t, "kill", victim)
		f.waitProgress("killing "+victim, 20)

		if n := f.placement()[victim]; n != 0 {
			t.Fatalf("%d sessions still answered by the killed worker", n)
		}
		s := f.stats()
		if got := s.RecoveriesSucceeded - before; got < uint64(onVictim) {
			t.Fatalf("%d recoveries for %d sessions on the killed worker", got, onVictim)
		}
		t.Logf("after kill: %v, %d recovered, %d duplicates dropped", f.placement(), s.RecoveriesSucceeded-before, s.DuplicatesDropped)
	})

	compose(t, "start", "worker-2")
	waitWorkers(t, composeGateway, 3)

	t.Run("redis outage", func(t *testing.T) {
		before := f.stats()

		compose(t, "stop", "redis")
		f.waitStats("snapshot failures to keep being reported", func(s stats) bool {
			return s.WorkerSnapshotFailures >= before.WorkerSnapshotFailures+6
		})
		f.waitProgress("redis being down", 20)

		migrated := f.stats().MigrationsSucceeded
		compose(t, "stop", "worker-3")
		f.waitProgress("draining worker-3 while redis is down", 20)
		if n := f.placement()["worker-3"]; n != 0 {
			t.Fatalf("%d sessions still on worker-3 after draining it during the outage", n)
		}
		if f.stats().MigrationsSucceeded == migrated {
			t.Fatal("no session migrated while redis was down")
		}

		late := openFleet(t, composeGateway, 2)
		late.waitProgress("a new session during the outage", 10)

		compose(t, "up", "-d", "--no-deps", "worker-3")
		waitWorkers(t, composeGateway, 3)
		if strings.Contains(compose(t, "ps", "redis"), "Up") {
			t.Fatal("redis came back during the outage; the worker restart did not happen without it")
		}

		compose(t, "start", "redis")
		written := f.stats().WorkerSnapshotsWritten
		f.waitStats("snapshots to resume", func(s stats) bool {
			return s.WorkerSnapshotsWritten > written
		})
		f.waitProgress("redis coming back", 20)
	})

	t.Run("snapshots really are in redis", func(t *testing.T) {
		keys := strings.Fields(compose(t, "exec", "-T", "redis", "redis-cli", "--scan", "--pattern", "snapshot:*"))
		if len(keys) == 0 {
			t.Fatal("no snapshot keys in redis")
		}

		fields := strings.Fields(compose(t, "exec", "-T", "redis", "redis-cli", "HGETALL", keys[0]))
		stored := map[string]string{}
		for i := 0; i+1 < len(fields); i += 2 {
			stored[fields[i]] = fields[i+1]
		}

		seq, err := strconv.ParseUint(stored["applied_seq"], 10, 64)
		if err != nil || seq == 0 {
			t.Fatalf("%s has applied_seq %q", keys[0], stored["applied_seq"])
		}
		if _, ok := stored["state"]; !ok {
			t.Fatalf("%s has no state field: %v", keys[0], stored)
		}
		t.Logf("%d snapshot keys; %s holds state=%s applied_seq=%d", len(keys), keys[0], stored["state"], seq)
	})

	f.check("the whole run")
	s := f.stats()
	if s.ResendGaps != 0 || s.RecoveriesFailed != 0 || s.MigrationsFailed != 0 {
		t.Fatalf("unexpected failures: %+v", s)
	}
	if int(s.SessionsActive) != len(f.sessions) {
		t.Fatalf("%d sessions active, wanted %d", s.SessionsActive, len(f.sessions))
	}
	t.Logf("final: %+v", s)
}
