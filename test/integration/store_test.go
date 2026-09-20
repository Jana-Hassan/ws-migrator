package integration

import (
	"testing"

	"github.com/Jana-Hassan/ws-migrator/store"
	"github.com/Jana-Hassan/ws-migrator/worker"
)

func newMemoryCluster(t *testing.T, workerCount int) *cluster {
	t.Helper()

	onCreate := func(sessionID string) worker.State { return &counter{} }
	return newClusterOn(t, workerCount, onCreate, store.NewMemory(), nil)
}

func TestMemoryStoreRunsTheWholeFlow(t *testing.T) {
	c := newMemoryCluster(t, 2)

	client := c.connect()
	first := client.sendExpectingNext("hello")
	client.sendExpectingNext("hello")

	target := c.otherWorker(first.worker)
	c.migrateOK(first.sessionID, target)
	client.waitForWorker(target)

	c.waitForSnapshot(first.sessionID, "3")

	c.worker(target).kill()
	recoveredOn := c.waitForSessionOff(first.sessionID, target)

	r := client.sendExpectingNext("after-crash")
	if r.worker != recoveredOn {
		t.Fatalf("reply came from %s, expected %s", r.worker, recoveredOn)
	}
	if r.count != 4 {
		t.Fatalf("count %d after recovery on the memory store, wanted 4", r.count)
	}
}

func TestWorkerWithoutStoreSkipsSnapshots(t *testing.T) {
	c := newMemoryCluster(t, 1)
	c.addWorkerWithoutStore("worker-nostore")

	client := c.connect()
	for i := 0; i < 3; i++ {
		client.sendExpectingNext("hello")
	}

	c.migrateOK(client.session, "worker-nostore")
	client.waitForWorker("worker-nostore")

	c.dropSnapshot(client.session)

	for i := 0; i < 3; i++ {
		client.sendExpectingNext("still-serving")
	}

	if state, ok := c.snapshot(client.session); ok {
		t.Fatalf("worker without a store wrote a snapshot: %q", state)
	}
}
