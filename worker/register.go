package worker

import (
	"context"
	"log"
	"time"
)

func (w *Worker) StartRegistration(ctx context.Context) {
	if w.gatewayAddr == "" {
		return
	}

	client, err := newGatewayClient(w.gatewayAddr)
	if err != nil {
		log.Printf("[worker-%s] registration client failed: %v", w.id, err)
		return
	}
	defer client.close()

	ticker := time.NewTicker(w.registerInterval)
	defer ticker.Stop()

	for {
		w.registerOnce(client)

		select {
		case <-ctx.Done():
			return
		case <-w.draining:
			return
		case <-ticker.C:
		}
	}
}

func (w *Worker) registerOnce(client *gatewayClient) {
	resp, err := client.register(w.id, w.advertiseWS, w.advertiseGRPC, w.registerToken)
	if err != nil {
		log.Printf("[worker-%s] register failed: %v", w.id, err)
		return
	}
	if !resp.Ok {
		log.Printf("[worker-%s] register rejected: %s", w.id, resp.Error)
	}
}

func (w *Worker) unregister() {
	if w.gatewayAddr == "" {
		return
	}

	client, err := newGatewayClient(w.gatewayAddr)
	if err != nil {
		log.Printf("[worker-%s] unregister client failed: %v", w.id, err)
		return
	}
	defer client.close()

	if _, err := client.unregister(w.id, w.registerToken); err != nil {
		log.Printf("[worker-%s] unregister failed: %v", w.id, err)
	}
}
