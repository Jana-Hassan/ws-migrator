package gateway

import (
	"sync"
	"sync/atomic"
)

type Metrics struct {
	sessionsStarted   atomic.Uint64
	sessionsEnded     atomic.Uint64
	sessionsRejected  atomic.Uint64
	messagesForwarded atomic.Uint64

	migrationsAttempted atomic.Uint64
	migrationsSucceeded atomic.Uint64
	migrationsFailed    atomic.Uint64

	recoveriesAttempted atomic.Uint64
	recoveriesSucceeded atomic.Uint64
	recoveriesFailed    atomic.Uint64

	resendsSent       atomic.Uint64
	resendGaps        atomic.Uint64
	duplicatesDropped atomic.Uint64

	snapshotWriteFailures atomic.Uint64

	workerMu     sync.Mutex
	workerReport map[string]workerHealth
}

type workerHealth struct {
	snapshotFailures uint64
	snapshotsWritten uint64
}

func (m *Metrics) recordWorkerHealth(workerID string, snapshotFailures, snapshotsWritten uint64) {
	m.workerMu.Lock()
	defer m.workerMu.Unlock()

	if m.workerReport == nil {
		m.workerReport = make(map[string]workerHealth)
	}
	m.workerReport[workerID] = workerHealth{snapshotFailures: snapshotFailures, snapshotsWritten: snapshotsWritten}
}

func (m *Metrics) forgetWorker(workerID string) {
	m.workerMu.Lock()
	defer m.workerMu.Unlock()
	delete(m.workerReport, workerID)
}

func (m *Metrics) workerTotals() (failures, written uint64) {
	m.workerMu.Lock()
	defer m.workerMu.Unlock()

	for _, report := range m.workerReport {
		failures += report.snapshotFailures
		written += report.snapshotsWritten
	}
	return failures, written
}

type Stats struct {
	SessionsActive    uint64 `json:"sessions_active"`
	SessionsStarted   uint64 `json:"sessions_started"`
	SessionsEnded     uint64 `json:"sessions_ended"`
	SessionsRejected  uint64 `json:"sessions_rejected"`
	MessagesForwarded uint64 `json:"messages_forwarded"`

	MigrationsAttempted uint64 `json:"migrations_attempted"`
	MigrationsSucceeded uint64 `json:"migrations_succeeded"`
	MigrationsFailed    uint64 `json:"migrations_failed"`

	RecoveriesAttempted uint64 `json:"recoveries_attempted"`
	RecoveriesSucceeded uint64 `json:"recoveries_succeeded"`
	RecoveriesFailed    uint64 `json:"recoveries_failed"`

	ResendsSent       uint64 `json:"resends_sent"`
	ResendGaps        uint64 `json:"resend_gaps"`
	DuplicatesDropped uint64 `json:"duplicates_dropped"`

	SnapshotWriteFailures uint64 `json:"snapshot_write_failures"`

	WorkerSnapshotFailures uint64 `json:"worker_snapshot_failures"`
	WorkerSnapshotsWritten uint64 `json:"worker_snapshots_written"`

	WorkersTotal    int `json:"workers_total"`
	WorkersHealthy  int `json:"workers_healthy"`
	WorkersDraining int `json:"workers_draining"`
}

func (s *Server) Stats() Stats {
	m := &s.metrics

	stats := Stats{
		SessionsActive:        uint64(s.registry.Count()),
		SessionsStarted:       m.sessionsStarted.Load(),
		SessionsEnded:         m.sessionsEnded.Load(),
		SessionsRejected:      m.sessionsRejected.Load(),
		MessagesForwarded:     m.messagesForwarded.Load(),
		MigrationsAttempted:   m.migrationsAttempted.Load(),
		MigrationsSucceeded:   m.migrationsSucceeded.Load(),
		MigrationsFailed:      m.migrationsFailed.Load(),
		RecoveriesAttempted:   m.recoveriesAttempted.Load(),
		RecoveriesSucceeded:   m.recoveriesSucceeded.Load(),
		RecoveriesFailed:      m.recoveriesFailed.Load(),
		ResendsSent:           m.resendsSent.Load(),
		ResendGaps:            m.resendGaps.Load(),
		DuplicatesDropped:     m.duplicatesDropped.Load(),
		SnapshotWriteFailures: m.snapshotWriteFailures.Load(),
	}

	stats.WorkerSnapshotFailures, stats.WorkerSnapshotsWritten = m.workerTotals()

	for _, w := range s.pool.All() {
		stats.WorkersTotal++
		if w.Healthy {
			stats.WorkersHealthy++
		}
		if w.Draining {
			stats.WorkersDraining++
		}
	}

	return stats
}
