package worker

import (
	"context"
	"log"
	"time"
)

func (w *Worker) StartSnapshots(ctx context.Context) {
	if w.store == nil {
		return
	}

	ticker := time.NewTicker(w.snapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.snapshotAll(ctx)
		}
	}
}

func (w *Worker) snapshotAll(ctx context.Context) {
	for _, s := range w.sessions.all() {
		state, ok := s.takeSnapshot()
		if !ok {
			continue
		}

		if err := w.store.SaveSnapshot(ctx, s.id, state); err != nil {
			log.Printf("[worker-%s] snapshot %s failed: %v", w.id, s.id, err)
		}
	}
}

func (s *session) takeSnapshot() ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.dirty || s.state == nil {
		return nil, false
	}

	state, err := s.state.Serialize()
	if err != nil {
		log.Printf("snapshot %s: serialize failed: %v", s.id, err)
		return nil, false
	}

	s.dirty = false
	return state, true
}
