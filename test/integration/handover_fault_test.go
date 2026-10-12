package integration

import (
	"sync"
	"testing"
	"time"
)

type traffic struct {
	t       *testing.T
	clients []*pipelineClient
	stop    chan struct{}
	wg      sync.WaitGroup
}

func startTraffic(t *testing.T, c *cluster, n int) *traffic {
	t.Helper()

	tr := &traffic{t: t, stop: make(chan struct{})}
	for i := 0; i < n; i++ {
		tr.clients = append(tr.clients, c.connectPipelineClient())
	}

	for _, pc := range tr.clients {
		tr.wg.Add(2)
		go func(pc *pipelineClient) {
			defer tr.wg.Done()
			pc.write(tr.stop, time.Millisecond)
		}(pc)
		go func(pc *pipelineClient) {
			defer tr.wg.Done()
			pc.read()
		}(pc)
	}

	waitFor(t, "every client has a session", func() bool {
		return int(c.stats().SessionsActive) == n
	})
	time.Sleep(100 * time.Millisecond)
	return tr
}

func (tr *traffic) finish() {
	tr.t.Helper()

	time.Sleep(200 * time.Millisecond)
	close(tr.stop)
	awaitAllReplies(tr.t, tr.clients)

	for _, pc := range tr.clients {
		pc.conn.Close()
	}
	tr.wg.Wait()
}

func migrateAll(c *cluster, target string) {
	for _, id := range c.sessionIDs() {
		c.migrate(id, target)
	}
}

func TestHandoverDialFailureResumesSource(t *testing.T) {
	c := newSlowCluster(t, 1, 2*time.Millisecond)
	tr := startTraffic(t, c, 8)

	c.addFakeWorker("unreachable")
	migrateAll(c, "unreachable")

	waitFor(t, "every dial failed", func() bool {
		return int(c.stats().MigrationsFailed) >= len(tr.clients)
	})

	tr.finish()

	stats := c.stats()
	t.Logf("dial failures: %d failed migrations, %d resends", stats.MigrationsFailed, stats.ResendsSent)
	if stats.MigrationsSucceeded != 0 {
		t.Fatalf("%d migrations succeeded onto an unreachable worker", stats.MigrationsSucceeded)
	}
}

func TestHandoverTargetDroppingConnectionLosesNothing(t *testing.T) {
	c := newSlowCluster(t, 1, 2*time.Millisecond)
	c.startHealth()
	tr := startTraffic(t, c, 8)

	c.addSlammingWorker("slam")
	migrateAll(c, "slam")

	waitFor(t, "every handover attempt settled", func() bool {
		s := c.stats()
		return int(s.MigrationsFailed+s.RecoveriesSucceeded) >= len(tr.clients)
	})

	if active := int(c.stats().SessionsActive); active != len(tr.clients) {
		t.Fatalf("%d sessions active, wanted %d", active, len(tr.clients))
	}

	tr.finish()

	stats := c.stats()
	t.Logf("target dropped connections: %d failed migrations, %d succeeded then recovered, %d resends",
		stats.MigrationsFailed, stats.RecoveriesSucceeded, stats.ResendsSent)
}

func TestHandoverSourceDyingMidwayRecovers(t *testing.T) {
	c := newSlowCluster(t, 1, 2*time.Millisecond)
	c.startHealth()
	tr := startTraffic(t, c, 8)

	c.addScaleWorker("healthy")
	c.addSlowRestoreWorker("slow", 300*time.Millisecond)

	migrateAll(c, "slow")
	time.Sleep(100 * time.Millisecond)
	c.worker("worker-1").kill()

	for _, id := range c.sessionIDs() {
		c.waitForSessionOff(id, "worker-1")
	}

	tr.finish()

	stats := c.stats()
	t.Logf("source died mid-handover: %d recoveries, %d failed recoveries", stats.RecoveriesSucceeded, stats.RecoveriesFailed)

	if stats.RecoveriesFailed != 0 {
		t.Fatalf("%d recoveries failed although a healthy worker was available", stats.RecoveriesFailed)
	}
}
