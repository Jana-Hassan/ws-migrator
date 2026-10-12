package worker

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/Jana-Hassan/ws-migrator/store"
)

type flakyStore struct {
	store.Store
	mu       sync.Mutex
	failures int
	saves    []store.Snapshot
}

func (f *flakyStore) SaveSnapshot(ctx context.Context, sessionID string, snap store.Snapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.failures > 0 {
		f.failures--
		return errors.New("redis unavailable")
	}
	f.saves = append(f.saves, snap)
	return nil
}

func TestFailedSnapshotIsRetried(t *testing.T) {
	flaky := &flakyStore{Store: store.NewMemory(), failures: 1}

	w, url := newCountWorker(t, "a")
	w.store = flaky

	gateway := dialAsGateway(t, url+"?session_id=s1")
	sendMessage(t, gateway, 1)
	readDown(t, gateway)

	w.snapshotAll(context.Background())
	if len(flaky.saves) != 0 || w.snapshotFailures.Load() != 1 {
		t.Fatalf("first save should fail: saves=%d failures=%d", len(flaky.saves), w.snapshotFailures.Load())
	}

	w.snapshotAll(context.Background())
	if len(flaky.saves) != 1 {
		t.Fatalf("the failed snapshot was not retried: %d saves", len(flaky.saves))
	}
	if string(flaky.saves[0].State) != "1" || flaky.saves[0].AppliedSeq != 1 {
		t.Fatalf("retried snapshot is %+v, wanted state 1 at seq 1", flaky.saves[0])
	}
}
