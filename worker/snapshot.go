package worker

import (
	"context"
	"log"
	"time"

	"github.com/Jana-Hassan/ws-migrator/store"
)

func (w *Worker) StartSnapshots(ctx context.Context) {
	ticker := time.NewTicker(w.snapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, id := range w.sessions.sweepPending(w.pendingTTL) {
				log.Printf("[worker-%s] dropped staged state for %s: connection never arrived", w.id, id)
			}
			if w.store != nil {
				w.snapshotAll(ctx)
			}
		}
	}
}

func (w *Worker) snapshotAll(ctx context.Context) {
	for _, s := range w.sessions.all() {
		snap, ok := s.takeSnapshot()
		if !ok {
			continue
		}

		if err := w.store.SaveSnapshot(ctx, s.id, snap); err != nil {
			w.snapshotFailures.Add(1)
			log.Printf("[worker-%s] snapshot %s failed: %v", w.id, s.id, err)
			continue
		}
		w.snapshotsWritten.Add(1)
	}
}

func (s *session) takeSnapshot() (store.Snapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.dirty || s.state == nil {
		return store.Snapshot{}, false
	}

	state, err := s.state.Serialize()
	if err != nil {
		log.Printf("snapshot %s: serialize failed: %v", s.id, err)
		return store.Snapshot{}, false
	}

	s.dirty = false
	return store.Snapshot{State: state, AppliedSeq: s.appliedSeq, Outbox: encodeOutbox(s.outbox)}, true
}
