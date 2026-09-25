package gateway

import (
	"fmt"
	"testing"
)

func checkCounts(t *testing.T, r *Registry, workers ...string) {
	t.Helper()

	for _, id := range workers {
		scanned := len(r.AllOnWorker(id))
		if counted := r.CountOnWorker(id); counted != scanned {
			t.Fatalf("worker %s: CountOnWorker=%d but AllOnWorker=%d", id, counted, scanned)
		}
	}
}

func TestCountOnWorkerTracksChurn(t *testing.T) {
	r := NewRegistry()
	workers := []string{"w1", "w2", "w3"}

	for i := 0; i < 30; i++ {
		r.Add(&Session{ID: fmt.Sprintf("s%d", i), WorkerID: workers[i%3]})
	}
	checkCounts(t, r, workers...)

	for i := 0; i < 30; i += 2 {
		r.UpdateWorker(fmt.Sprintf("s%d", i), "w3")
	}
	checkCounts(t, r, workers...)

	for i := 0; i < 30; i += 3 {
		r.Remove(fmt.Sprintf("s%d", i))
	}
	checkCounts(t, r, workers...)

	if got := r.CountOnWorker("never-existed"); got != 0 {
		t.Fatalf("unknown worker reported %d sessions", got)
	}
}

func TestCountOnWorkerIgnoresNoOpUpdates(t *testing.T) {
	r := NewRegistry()
	r.Add(&Session{ID: "s1", WorkerID: "w1"})

	r.UpdateWorker("s1", "w1")
	r.UpdateWorker("missing", "w2")

	checkCounts(t, r, "w1", "w2")
	if got := r.CountOnWorker("w1"); got != 1 {
		t.Fatalf("w1 holds %d sessions, wanted 1", got)
	}
}

func TestCountOnWorkerHandlesReAdd(t *testing.T) {
	r := NewRegistry()
	r.Add(&Session{ID: "s1", WorkerID: "w1"})
	r.Add(&Session{ID: "s1", WorkerID: "w2"})

	checkCounts(t, r, "w1", "w2")
	if got := r.CountOnWorker("w1"); got != 0 {
		t.Fatalf("w1 holds %d sessions after re-add, wanted 0", got)
	}
}
