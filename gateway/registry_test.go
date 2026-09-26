package gateway

import (
	"fmt"
	"testing"

	"github.com/Jana-Hassan/ws-migrator/internal/frame"
)

type expectedPlacement struct {
	t       *testing.T
	r       *Registry
	byID    map[string]string
	workers map[string]bool
}

func newExpectedPlacement(t *testing.T, r *Registry) *expectedPlacement {
	return &expectedPlacement{t: t, r: r, byID: map[string]string{}, workers: map[string]bool{}}
}

func (e *expectedPlacement) add(id, worker string) {
	e.r.Add(&Session{ID: id, WorkerID: worker})
	e.byID[id] = worker
	e.workers[worker] = true
}

func (e *expectedPlacement) move(id, worker string) {
	e.r.UpdateWorker(id, worker)
	if _, known := e.byID[id]; known {
		e.byID[id] = worker
	}
	e.workers[worker] = true
}

func (e *expectedPlacement) remove(id string) {
	e.r.Remove(id)
	delete(e.byID, id)
}

func (e *expectedPlacement) check() {
	e.t.Helper()

	want := map[string]int{}
	for _, worker := range e.byID {
		want[worker]++
	}

	for worker := range e.workers {
		if got := e.r.CountOnWorker(worker); got != want[worker] {
			e.t.Fatalf("worker %s: CountOnWorker=%d, expected %d", worker, got, want[worker])
		}
		if got := len(e.r.AllOnWorker(worker)); got != want[worker] {
			e.t.Fatalf("worker %s: AllOnWorker=%d, expected %d", worker, got, want[worker])
		}
	}

	if got := e.r.Count(); got != len(e.byID) {
		e.t.Fatalf("Count=%d, expected %d", got, len(e.byID))
	}
}

func TestRegistryCountsSurviveChurn(t *testing.T) {
	r := NewRegistry()
	e := newExpectedPlacement(t, r)

	workers := []string{"w1", "w2", "w3"}
	for i := 0; i < 30; i++ {
		e.add(fmt.Sprintf("s%d", i), workers[i%3])
	}
	e.check()

	for i := 0; i < 30; i += 2 {
		e.move(fmt.Sprintf("s%d", i), "w3")
	}
	e.check()

	for i := 0; i < 30; i += 3 {
		e.remove(fmt.Sprintf("s%d", i))
	}
	e.check()
}

func TestRegistryCountsIgnoreStaleSessionField(t *testing.T) {
	r := NewRegistry()
	e := newExpectedPlacement(t, r)

	e.add("s1", "w1")

	session, ok := r.Get("s1")
	if !ok {
		t.Fatal("session missing")
	}

	session.WorkerID = "w2"
	e.move("s1", "w2")
	e.check()

	if got := r.CountOnWorker("w1"); got != 0 {
		t.Fatalf("w1 still counts %d sessions after the move", got)
	}
	if got := r.CountOnWorker("w2"); got != 1 {
		t.Fatalf("w2 counts %d sessions, wanted 1", got)
	}
}

func TestRegistryHandlesUnknownAndRepeatedUpdates(t *testing.T) {
	r := NewRegistry()
	e := newExpectedPlacement(t, r)

	e.add("s1", "w1")
	e.move("s1", "w1")
	e.move("missing", "w2")
	e.check()

	e.add("s1", "w2")
	e.check()

	e.remove("s1")
	e.remove("s1")
	e.check()
}

func TestReplyAndPushWatermarksAreIndependent(t *testing.T) {
	s := &Session{}

	steps := []struct {
		kind  byte
		key   uint64
		admit bool
	}{
		{frame.Reply, 5, true},
		{frame.Push, 1, true},
		{frame.Reply, 6, true},
		{frame.Push, 2, true},
		{frame.Push, 3, true},
		{frame.Reply, 5, false},
		{frame.Push, 2, false},
		{frame.Reply, 7, true},
		{frame.Push, 4, true},
		{frame.Plain, 0, true},
		{frame.Plain, 0, true},
	}

	for i, step := range steps {
		if got := s.admitDownlink(step.kind, step.key); got != step.admit {
			t.Fatalf("step %d: kind %d key %d admitted=%v, wanted %v", i, step.kind, step.key, got, step.admit)
		}
	}

	if s.lastReply != 7 || s.lastPush != 4 {
		t.Fatalf("watermarks reply=%d push=%d, wanted 7 and 4", s.lastReply, s.lastPush)
	}
}
