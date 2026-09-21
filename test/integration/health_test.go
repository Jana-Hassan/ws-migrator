package integration

import (
	"testing"
	"time"

	"github.com/Jana-Hassan/ws-migrator/worker"
)

func TestStreamCloseMarksWorkerUnhealthy(t *testing.T) {
	c := newCluster(t, 1)
	fake := c.addFakeWorker("fake-1")
	c.startHealth()

	fake.beat(t)
	fake.beat(t)
	waitFor(t, "fake-1 healthy", func() bool { return c.healthy("fake-1") })

	fake.stop()
	waitFor(t, "fake-1 unhealthy", func() bool { return !c.healthy("fake-1") })
}

func TestSilentWorkerBecomesUnhealthy(t *testing.T) {
	c := newCluster(t, 1)
	fake := c.addFakeWorker("fake-1")
	c.startHealth()

	fake.beat(t)
	fake.beat(t)
	waitFor(t, "fake-1 healthy", func() bool { return c.healthy("fake-1") })

	waitFor(t, "fake-1 unhealthy after silence", func() bool { return !c.healthy("fake-1") })
}

func TestBriefGapDoesNotFlipHealth(t *testing.T) {
	c := newCluster(t, 1)
	fake := c.addFakeWorker("fake-1")
	c.startHealth()

	fake.beat(t)
	fake.beat(t)
	waitFor(t, "fake-1 healthy", func() bool { return c.healthy("fake-1") })

	for i := 0; i < 4; i++ {
		time.Sleep(testHealthConfig.Timeout / 2)
		fake.beat(t)
		if !c.healthy("fake-1") {
			t.Fatalf("worker flipped unhealthy during a gap shorter than the timeout")
		}
	}
}

func TestUnhealthyWorkerRecovers(t *testing.T) {
	c := newCluster(t, 1)
	fake := c.addFakeWorker("fake-1")
	c.startHealth()

	waitFor(t, "fake-1 unhealthy", func() bool { return !c.healthy("fake-1") })

	fake.beat(t)
	fake.beat(t)
	waitFor(t, "fake-1 healthy again", func() bool { return c.healthy("fake-1") })
}

func TestUnhealthyWorkerGetsNoNewSessions(t *testing.T) {
	c := newCluster(t, 1)
	c.addWorker("worker-slow", func(string) worker.State { return &counter{} }, slowHealthInterval)
	c.startHealth()

	waitFor(t, "worker-slow unhealthy", func() bool { return !c.healthy("worker-slow") })

	for i := 0; i < 5; i++ {
		client := c.connect()
		if r := client.sendExpectingNext("hello"); r.worker != "worker-1" {
			t.Fatalf("new session landed on %s, wanted worker-1", r.worker)
		}
	}
}

func TestSessionsOnUnhealthyWorkerKeepWorking(t *testing.T) {
	c := newCluster(t, 1)
	c.addWorker("worker-slow", func(string) worker.State { return &counter{} }, slowHealthInterval)

	client := c.connect()
	first := client.sendExpectingNext("hello")
	c.migrateOK(first.sessionID, "worker-slow")
	client.waitForWorker("worker-slow")

	c.startHealth()
	waitFor(t, "worker-slow unhealthy", func() bool { return !c.healthy("worker-slow") })

	for i := 0; i < 3; i++ {
		if r := client.sendExpectingNext("still-here"); r.worker != "worker-slow" {
			t.Fatalf("session moved to %s on its own", r.worker)
		}
	}
}

func TestHeartbeatsDoNotUndoDrain(t *testing.T) {
	c := newCluster(t, 2)
	c.startHealth()

	probe := c.connect()
	victim := probe.sendExpectingNext("hello").worker
	survivor := c.otherWorker(victim)

	c.worker(victim).worker.Drain("test")

	time.Sleep(3 * testHealthInterval)

	for i := 0; i < 3; i++ {
		client := c.connect()
		if r := client.sendExpectingNext("hello"); r.worker != survivor {
			t.Fatalf("session landed on drained worker %s", r.worker)
		}
	}
}
