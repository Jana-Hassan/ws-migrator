package gateway

import (
	"sync"
)

type WorkerInfo struct {
	ID       string
	WSAddr   string
	GRPCAddr string
	Draining bool
	Healthy  bool
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

	added := *w
	added.Healthy = true
	p.workers[added.ID] = &added
}

func (p *Pool) Get(id string) (WorkerInfo, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	w, ok := p.workers[id]
	if !ok {
		return WorkerInfo{}, false
	}
	return *w, true
}

func (p *Pool) Update(w *WorkerInfo) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	existing, ok := p.workers[w.ID]
	if !ok {
		added := *w
		added.Healthy = true
		p.workers[added.ID] = &added
		return true
	}

	changed := existing.WSAddr != w.WSAddr || existing.GRPCAddr != w.GRPCAddr
	existing.WSAddr = w.WSAddr
	existing.GRPCAddr = w.GRPCAddr
	return changed
}

func (p *Pool) Remove(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.workers, id)
}

func (p *Pool) SetDraining(id string, draining bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if w, ok := p.workers[id]; ok {
		w.Draining = draining
	}
}

func (p *Pool) SetHealthy(id string, healthy bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if w, ok := p.workers[id]; ok {
		w.Healthy = healthy
	}
}

func (p *Pool) All() []WorkerInfo {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make([]WorkerInfo, 0, len(p.workers))
	for _, w := range p.workers {
		result = append(result, *w)
	}
	return result
}

func (p *Pool) Count() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.workers)
}
