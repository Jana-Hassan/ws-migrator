package integration

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Jana-Hassan/ws-migrator/worker"
)

type hookEvent struct {
	worker  string
	session string
	kind    string
	resumed bool
}

type hookLog struct {
	mu     sync.Mutex
	events []hookEvent
}

func (h *hookLog) add(e hookEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, e)
}

func (h *hookLog) forSession(session string) []hookEvent {
	h.mu.Lock()
	defer h.mu.Unlock()

	var out []hookEvent
	for _, e := range h.events {
		if e.session == session {
			out = append(out, e)
		}
	}
	return out
}

func newHookCluster(t *testing.T, workers int) (*cluster, *hookLog) {
	t.Helper()

	log := &hookLog{}
	c := newEmptyCluster(t)
	c.snapshotInterval = time.Hour
	c.configureWorker = func(id string, w *worker.Worker) {
		w.OnSessionReady(func(r worker.Ready) {
			log.add(hookEvent{worker: id, session: r.SessionID, kind: "ready", resumed: r.Resumed})
		})

		w.OnMessage(func(sessionID string, state worker.State, msg []byte) []byte {
			log.add(hookEvent{worker: id, session: sessionID, kind: "message"})
			ctr := state.(*counter)
			ctr.Count++
			return []byte(fmt.Sprintf("worker=%s session=%s count=%d", id, sessionID, ctr.Count))
		})

		w.OnSessionEnd(func(sessionID string, state worker.State) {
			log.add(hookEvent{worker: id, session: sessionID, kind: "end"})
		})
	}

	for i := 1; i <= workers; i++ {
		c.addScaleWorker(fmt.Sprintf("worker-%d", i))
	}
	return c, log
}

func eventsOn(events []hookEvent, worker string) []hookEvent {
	var out []hookEvent
	for _, e := range events {
		if e.worker == worker {
			out = append(out, e)
		}
	}
	return out
}

func TestReadyFiresOnceBeforeMessagesAndEnd(t *testing.T) {
	c, log := newHookCluster(t, 1)

	client := c.connect()
	first := client.sendExpectingNext("hello")
	client.sendExpectingNext("hello")
	client.conn.Close()

	waitFor(t, "session end recorded", func() bool {
		events := log.forSession(first.sessionID)
		return len(events) > 0 && events[len(events)-1].kind == "end"
	})

	events := log.forSession(first.sessionID)
	kinds := make([]string, len(events))
	for i, e := range events {
		kinds[i] = e.kind
	}

	want := []string{"ready", "message", "message", "end"}
	if fmt.Sprint(kinds) != fmt.Sprint(want) {
		t.Fatalf("hook order %v, wanted %v", kinds, want)
	}
	if events[0].resumed {
		t.Fatal("a brand-new session was reported as resumed")
	}
}

func TestReadyReportsResumedAfterDrain(t *testing.T) {
	c, log := newHookCluster(t, 2)

	client := c.connect()
	first := client.sendExpectingNext("hello")
	target := c.otherWorker(first.worker)

	c.worker(first.worker).worker.Drain("hook test")
	client.waitForWorker(target)

	onTarget := eventsOn(log.forSession(first.sessionID), target)
	if len(onTarget) == 0 || onTarget[0].kind != "ready" {
		t.Fatalf("first event on %s was %+v, wanted ready", target, onTarget)
	}
	if !onTarget[0].resumed {
		t.Fatal("a migrated session was not reported as resumed")
	}

	readies := 0
	for _, e := range onTarget {
		if e.kind == "ready" {
			readies++
		}
	}
	if readies != 1 {
		t.Fatalf("%d ready events on %s, wanted 1", readies, target)
	}
}

func TestReadyReportsResumedAfterCrashWithoutSnapshot(t *testing.T) {
	c, log := newHookCluster(t, 2)
	c.startHealth()

	client := c.connect()
	first := client.sendExpectingNext("hello")

	if state, ok := c.snapshot(first.sessionID); ok {
		t.Fatalf("setup failed: a snapshot exists (%q), so this would not test the snapshot-less case", state)
	}

	c.worker(first.worker).kill()
	landed := c.waitForSessionOff(first.sessionID, first.worker)

	waitFor(t, "ready on the recovery target", func() bool {
		for _, e := range eventsOn(log.forSession(first.sessionID), landed) {
			if e.kind == "ready" {
				return true
			}
		}
		return false
	})

	for _, e := range eventsOn(log.forSession(first.sessionID), landed) {
		if e.kind == "ready" && !e.resumed {
			t.Fatal("a recovered session was reported as brand new")
		}
	}
}

func TestUpdateMarksSessionDirty(t *testing.T) {
	c := newCluster(t, 1)

	client := c.connect()
	first := client.sendExpectingNext("hello")
	c.waitForSnapshot(first.sessionID, "1")

	w := c.worker(first.worker).worker
	err := w.Update(first.sessionID, func(state worker.State) {
		state.(*counter).Count = 42
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}

	c.waitForSnapshot(first.sessionID, "42")
}

func TestUpdateOnUnknownSessionFails(t *testing.T) {
	c := newCluster(t, 1)

	err := c.workers[0].worker.Update("sess_missing", func(worker.State) {})
	if err == nil {
		t.Fatal("update on an unknown session should fail")
	}
}

func TestUpdateIsSerializedWithMessages(t *testing.T) {
	c := newCluster(t, 1)

	client := c.connect()
	first := client.sendExpectingNext("hello")
	w := c.worker(first.worker).worker

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.Update(first.sessionID, func(state worker.State) {
				state.(*counter).Count++
			})
		}()
	}

	for i := 0; i < 50; i++ {
		client.send("tick")
	}
	wg.Wait()

	r := client.send("final")
	if r.count != 102 {
		t.Fatalf("count %d after 1+50 messages, 50 updates and 1 final, wanted 102", r.count)
	}
}
