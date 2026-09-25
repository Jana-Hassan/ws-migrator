package worker

import (
	"testing"
	"time"
)

func TestStagedStateIsTakenOnce(t *testing.T) {
	r := newSessionRegistry()
	r.stage("sess-1", &testState{}, 7)

	state, appliedSeq, ok := r.take("sess-1")
	if !ok || state == nil {
		t.Fatal("staged state was not returned")
	}
	if appliedSeq != 7 {
		t.Fatalf("applied seq %d, wanted 7", appliedSeq)
	}
	if _, _, ok := r.take("sess-1"); ok {
		t.Fatal("staged state was returned twice")
	}
}

func TestStagedStateExpires(t *testing.T) {
	r := newSessionRegistry()
	r.stage("sess-1", &testState{}, 7)

	if dropped := r.sweepPending(time.Hour); len(dropped) != 0 {
		t.Fatalf("swept %v before the TTL", dropped)
	}

	time.Sleep(20 * time.Millisecond)

	dropped := r.sweepPending(10 * time.Millisecond)
	if len(dropped) != 1 || dropped[0] != "sess-1" {
		t.Fatalf("sweep returned %v, wanted [sess-1]", dropped)
	}
	if _, _, ok := r.take("sess-1"); ok {
		t.Fatal("expired state is still staged")
	}
}

type testState struct{}

func (t *testState) Serialize() ([]byte, error) { return nil, nil }
func (t *testState) Restore([]byte) error       { return nil }
