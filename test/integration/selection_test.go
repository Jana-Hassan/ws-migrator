package integration

import (
	"testing"
	"time"
)

func connectClients(c *cluster, n int) []*testClient {
	c.t.Helper()

	clients := make([]*testClient, n)
	for i := range clients {
		clients[i] = c.connect()
		clients[i].sendExpectingNext("hello")
	}
	return clients
}

func pileOnto(c *cluster, target string, clients []*testClient) {
	c.t.Helper()

	for _, client := range clients {
		if client.worker() == target {
			continue
		}
		c.migrateOK(client.session, target)
		client.waitForWorker(target)
	}
}

func maxLoad(counts map[string]int) (string, int) {
	worst, highest := "", 0
	for id, n := range counts {
		if n > highest {
			worst, highest = id, n
		}
	}
	return worst, highest
}

func TestDrainSpreadsSessionsAcrossLeastLoadedWorkers(t *testing.T) {
	c := newCluster(t, 6)

	clients := connectClients(c, 24)
	c.reportDistribution("initial", c.distribution(clients))

	heavy := clients[0].worker()
	pileOnto(c, heavy, clients[:12])

	before := c.distribution(clients)
	c.reportDistribution("before drain (uneven)", before)
	if before[heavy] < len(clients)/2 {
		t.Fatalf("setup failed: %s holds only %d of %d sessions", heavy, before[heavy], len(clients))
	}

	c.worker(heavy).worker.Drain("test")

	after := c.distribution(clients)
	c.reportDistribution("after draining "+heavy, after)

	if after[heavy] != 0 {
		t.Fatalf("%s still holds %d sessions after drain", heavy, after[heavy])
	}

	survivors := len(c.workers) - 1
	ideal := len(clients) / survivors
	worst, highest := maxLoad(after)

	if len(after) != survivors {
		t.Fatalf("sessions landed on %d workers, wanted all %d survivors", len(after), survivors)
	}
	if highest > ideal*2 {
		t.Fatalf("%s took %d sessions, more than twice the even share of %d", worst, highest, ideal)
	}
}

func TestSequentialDrainsKeepFindingTargets(t *testing.T) {
	c := newCluster(t, 6)

	clients := connectClients(c, 18)
	c.reportDistribution("initial", c.distribution(clients))

	drained := make(map[string]bool)
	for i := 0; i < 3; i++ {
		victim := clients[0].worker()
		for drained[victim] {
			victim = c.workers[i].id
		}

		c.worker(victim).worker.Drain("test")
		drained[victim] = true

		counts := c.distribution(clients)
		c.reportDistribution("after draining "+victim, counts)

		for id := range drained {
			if counts[id] != 0 {
				t.Fatalf("drained worker %s still holds %d sessions", id, counts[id])
			}
		}
	}
}

func TestCrashDuringDrainStillRecovers(t *testing.T) {
	c := newClusterDrainTimeout(t, 6, 5*time.Second)
	c.startHealth()

	clients := connectClients(c, 18)

	draining := clients[0].worker()
	var victim string
	for _, w := range c.workers {
		if w.id != draining {
			victim = w.id
			break
		}
	}

	c.reportDistribution("before", c.distribution(clients))

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.worker(draining).worker.Drain("test")
	}()

	c.worker(victim).kill()
	<-done

	counts := c.distributionAfterCrash(clients)
	c.reportDistribution("after drain+crash", counts)

	alive := 0
	for _, n := range counts {
		alive += n
	}
	if alive != len(clients) {
		t.Fatalf("%d of %d clients still served", alive, len(clients))
	}

	if counts[victim] != 0 {
		t.Fatalf("crashed worker %s still holds %d sessions", victim, counts[victim])
	}
	if counts[draining] > 1 {
		t.Fatalf("draining worker %s still holds %d sessions", draining, counts[draining])
	}
	if counts[draining] == 1 {
		t.Logf("one session stayed on %s: its drain target died mid-drain and the drain timed out", draining)
	}
}

func TestDrainingWorkerIsSkippedEvenWhenLeastLoaded(t *testing.T) {
	c := newCluster(t, 6)

	clients := connectClients(c, 12)

	idle := c.workers[5].id
	for _, client := range clients {
		if client.worker() == idle {
			c.migrateOK(client.session, c.workers[0].id)
			client.waitForWorker(c.workers[0].id)
		}
	}

	c.worker(idle).worker.Drain("test")

	heavy := clients[0].worker()
	pileOnto(c, heavy, clients[:6])
	c.reportDistribution("before drain", c.distribution(clients))

	c.worker(heavy).worker.Drain("test")

	counts := c.distribution(clients)
	c.reportDistribution("after draining "+heavy, counts)

	if counts[idle] != 0 {
		t.Fatalf("draining worker %s received %d sessions despite being least loaded", idle, counts[idle])
	}
}

func TestDrainRetriesWhenFirstTargetFails(t *testing.T) {
	c := newCluster(t, 3)

	clients := connectClients(c, 9)

	heavy := clients[0].worker()
	pileOnto(c, heavy, clients)
	c.reportDistribution("before drain", c.distribution(clients))

	c.addRestoreRejectingWorker("broken")

	c.worker(heavy).worker.Drain("test")

	counts := c.distribution(clients)
	c.reportDistribution("after draining "+heavy, counts)

	if counts["broken"] != 0 {
		t.Fatalf("broken worker accepted %d sessions", counts["broken"])
	}
	if counts[heavy] != 0 {
		t.Fatalf("%s still holds %d sessions: the drain did not retry past the broken target", heavy, counts[heavy])
	}
}

func TestSessionsStayBalancedAfterChurn(t *testing.T) {
	c := newCluster(t, 6)

	clients := connectClients(c, 30)
	c.reportDistribution("initial", c.distribution(clients))

	c.worker(c.workers[0].id).worker.Drain("test")
	c.reportDistribution("after drain 1", c.distribution(clients))

	c.worker(c.workers[1].id).worker.Drain("test")

	counts := c.distribution(clients)
	c.reportDistribution("after drain 2", counts)

	survivors := 4
	ideal := len(clients) / survivors
	worst, highest := maxLoad(counts)

	t.Logf("survivors=%d ideal=%d worst=%s highest=%d", survivors, ideal, worst, highest)

	if highest > ideal*2 {
		t.Fatalf("%s holds %d sessions, more than twice the even share of %d", worst, highest, ideal)
	}
}
