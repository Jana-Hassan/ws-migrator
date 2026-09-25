package gateway

import (
	"context"
	"log"
	"time"
)

const (
	defaultHealthTimeout  = 15 * time.Second
	defaultRecoverUpdates = 2
	defaultMinBackoff     = 1 * time.Second
	defaultMaxBackoff     = 30 * time.Second

	defaultReconcileInterval = 1 * time.Second
	defaultEvictAfter        = 60 * time.Second
)

type HealthConfig struct {
	Timeout             time.Duration
	RecoverUpdates      int
	MinBackoff          time.Duration
	MaxBackoff          time.Duration
	ReconcileInterval   time.Duration
	EvictAfterUnhealthy time.Duration
}

func (c *HealthConfig) applyDefaults() {
	if c.Timeout == 0 {
		c.Timeout = defaultHealthTimeout
	}
	if c.RecoverUpdates == 0 {
		c.RecoverUpdates = defaultRecoverUpdates
	}
	if c.MinBackoff == 0 {
		c.MinBackoff = defaultMinBackoff
	}
	if c.MaxBackoff == 0 {
		c.MaxBackoff = defaultMaxBackoff
	}
	if c.ReconcileInterval == 0 {
		c.ReconcileInterval = defaultReconcileInterval
	}
	if c.EvictAfterUnhealthy == 0 {
		c.EvictAfterUnhealthy = defaultEvictAfter
	}
}

func (s *Server) StartHealthMonitor(ctx context.Context, cfg HealthConfig) {
	cfg.applyDefaults()
	go s.reconcileMonitors(ctx, cfg)
}

func (s *Server) reconcileMonitors(ctx context.Context, cfg HealthConfig) {
	monitored := make(map[string]context.CancelFunc)
	unhealthySince := make(map[string]time.Time)

	defer func() {
		for _, cancel := range monitored {
			cancel()
		}
	}()

	ticker := time.NewTicker(cfg.ReconcileInterval)
	defer ticker.Stop()

	for {
		present := make(map[string]bool)

		for _, w := range s.pool.All() {
			present[w.ID] = true

			if _, ok := monitored[w.ID]; !ok {
				workerCtx, cancel := context.WithCancel(ctx)
				monitored[w.ID] = cancel
				go s.monitorWorker(workerCtx, w.ID, cfg)
			}

			if w.Healthy {
				delete(unhealthySince, w.ID)
				continue
			}

			since, ok := unhealthySince[w.ID]
			if !ok {
				unhealthySince[w.ID] = time.Now()
				continue
			}
			if time.Since(since) >= cfg.EvictAfterUnhealthy {
				log.Printf("health: evicting %s after %s unhealthy", w.ID, cfg.EvictAfterUnhealthy)
				s.pool.Remove(w.ID)
				s.grpcClient.Invalidate(w.ID)
				s.metrics.forgetWorker(w.ID)
			}
		}

		for id, cancel := range monitored {
			if !present[id] {
				cancel()
				delete(monitored, id)
				delete(unhealthySince, id)
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) monitorWorker(ctx context.Context, workerID string, cfg HealthConfig) {
	backoff := cfg.MinBackoff

	for ctx.Err() == nil {
		sawUpdate, err := s.watchHealth(ctx, workerID, cfg)
		if ctx.Err() != nil {
			return
		}

		s.pool.SetHealthy(workerID, false)
		log.Printf("health: %s unhealthy: %v", workerID, err)

		if sawUpdate {
			backoff = cfg.MinBackoff
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > cfg.MaxBackoff {
			backoff = cfg.MaxBackoff
		}
	}
}

func (s *Server) watchHealth(ctx context.Context, workerID string, cfg HealthConfig) (bool, error) {
	stream, err := s.grpcClient.HealthStream(ctx, workerID)
	if err != nil {
		return false, err
	}

	updates := make(chan struct{})
	errs := make(chan error, 1)

	go func() {
		for {
			update, err := stream.Recv()
			if err != nil {
				errs <- err
				return
			}
			s.metrics.recordWorkerHealth(workerID, update.SnapshotFailures, update.SnapshotsWritten)
			select {
			case updates <- struct{}{}:
			case <-ctx.Done():
				return
			}
		}
	}()

	sawUpdate := false
	seen := 0

	timer := time.NewTimer(cfg.Timeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return sawUpdate, ctx.Err()

		case err := <-errs:
			return sawUpdate, err

		case <-timer.C:
			log.Printf("health: %s silent for %s", workerID, cfg.Timeout)
			s.pool.SetHealthy(workerID, false)
			seen = 0
			timer.Reset(cfg.Timeout)

		case <-updates:
			sawUpdate = true
			seen++
			if seen >= cfg.RecoverUpdates {
				s.pool.SetHealthy(workerID, true)
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(cfg.Timeout)
		}
	}
}
