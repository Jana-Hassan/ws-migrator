package integration

import (
	"testing"
	"time"
)

func (c *cluster) waitForPool(id string, want bool) {
	c.t.Helper()

	label := "worker " + id + " in pool"
	if !want {
		label = "worker " + id + " gone from pool"
	}

	waitFor(c.t, label, func() bool {
		_, ok := c.pool.Get(id)
		return ok == want
	})
}

func TestRegisteredWorkerReceivesSessions(t *testing.T) {
	c := newEmptyCluster(t)
	c.addSelfRegisteringWorker("worker-late")

	c.waitForPool("worker-late", true)

	client := c.connect()
	if r := client.sendExpectingNext("hello"); r.worker != "worker-late" {
		t.Fatalf("session landed on %s", r.worker)
	}
}

func TestLateWorkerIsHealthMonitored(t *testing.T) {
	c := newCluster(t, 1)
	c.startHealth()

	fake := c.addFakeWorker("fake-late")
	fake.beat(t)
	fake.beat(t)
	waitFor(t, "fake-late healthy", func() bool { return c.healthyInPool("fake-late") })

	fake.stop()
	waitFor(t, "fake-late unhealthy", func() bool { return !c.healthyInPool("fake-late") })
}

func TestUnhealthyWorkerIsEvicted(t *testing.T) {
	c := newCluster(t, 1)
	c.addFakeWorker("fake-doomed")
	c.startHealth()

	c.waitForPool("fake-doomed", false)
}

func TestUnregisterRemovesWorker(t *testing.T) {
	c := newCluster(t, 2)

	victim := c.workers[1]
	victim.worker.Drain("test")

	c.waitForPool(victim.id, false)

	for i := 0; i < 3; i++ {
		client := c.connect()
		if r := client.sendExpectingNext("hello"); r.worker == victim.id {
			t.Fatalf("session landed on unregistered worker %s", r.worker)
		}
	}
}

func TestRegisterIsIdempotent(t *testing.T) {
	c := newCluster(t, 1)
	tw := c.addSelfRegisteringWorker("worker-repeat")

	c.waitForPool("worker-repeat", true)
	time.Sleep(4 * testRegisterInterval)

	if c.pool.Count() != 2 {
		t.Fatalf("pool has %d workers, wanted 2", c.pool.Count())
	}

	tw.worker.Drain("test")
	c.waitForPool("worker-repeat", false)
}

func TestDrainSurvivesReRegistration(t *testing.T) {
	c := newCluster(t, 2)

	probe := c.connect()
	victim := probe.sendExpectingNext("hello").worker
	survivor := c.otherWorker(victim)

	c.worker(victim).worker.Drain("test")

	if w, ok := c.pool.Get(survivor); !ok || w.Draining {
		t.Fatalf("survivor %s should be usable", survivor)
	}

	for i := 0; i < 3; i++ {
		client := c.connect()
		if r := client.sendExpectingNext("hello"); r.worker != survivor {
			t.Fatalf("session landed on %s, wanted %s", r.worker, survivor)
		}
	}
}
