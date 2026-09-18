package gateway

import (
	"sync"
)

type WorkerInfo struct {
	ID       string
	WSAddr   string
	GRPCAddr string
	Draining bool
}

type Pool struct {
	mu      sync.RWMutex
	workers map[string]*WorkerInfo
}

func NewPool() *Pool {
	return &Pool{
		workers: make(map[string]*WorkerInfo),
	}
}

func (p *Pool) Add(w *WorkerInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.workers[w.ID] = w
}

func (p *Pool) Get(id string) (*WorkerInfo, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	w, ok := p.workers[id]
	return w, ok
}

func (p *Pool) SetDraining(id string, draining bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if w, ok := p.workers[id]; ok {
		w.Draining = draining
	}
}

func (p *Pool) All() []*WorkerInfo {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make([]*WorkerInfo, 0, len(p.workers))
	for _, w := range p.workers {
		result = append(result, w)
	}
	return result
}

func (p *Pool) Count() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.workers)
}