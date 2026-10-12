//go:build e2e

package e2e

import (
	"fmt"
	"go/build"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

const (
	kindCluster = "wsm-e2e"
	kubeContext = "kind-" + kindCluster
	namespace   = "wsm-e2e"
	k8sGateway  = "http://localhost:18090"
	k8sWorkers  = 6
	k8sSessions = 60
)

func kindBin(t *testing.T) string {
	t.Helper()

	if path, err := exec.LookPath("kind"); err == nil {
		return path
	}
	name := "kind"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	path := filepath.Join(build.Default.GOPATH, "bin", name)
	if _, err := os.Stat(path); err != nil {
		t.Skip("kind is not installed (go install sigs.k8s.io/kind@latest)")
	}
	return path
}

func kubectl(t *testing.T, args ...string) string {
	t.Helper()
	return sh(t, "kubectl", append([]string{"--context", kubeContext, "-n", namespace}, args...)...)
}

func workerPods(t *testing.T) []string {
	t.Helper()
	out := kubectl(t, "get", "pods", "-l", "app=ws-worker", "--field-selector=status.phase=Running",
		"-o", "jsonpath={.items[*].metadata.name}")
	return strings.Fields(out)
}

func portForward(t *testing.T) {
	t.Helper()

	cmd := exec.Command("kubectl", "--context", kubeContext, "-n", namespace, "port-forward", "svc/ws-gateway", "18090:8080")
	if err := cmd.Start(); err != nil {
		t.Fatalf("port-forward: %v", err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})
	waitHTTP(t, k8sGateway+"/health", time.Minute)
}

func TestKubernetes(t *testing.T) {
	kind := kindBin(t)

	if !strings.Contains(sh(t, kind, "get", "clusters"), kindCluster) {
		sh(t, kind, "create", "cluster", "--name", kindCluster, "--wait", "180s")
	}
	if os.Getenv("E2E_KEEP_CLUSTER") == "" {
		t.Cleanup(func() { sh(t, kind, "delete", "cluster", "--name", kindCluster) })
	}

	sh(t, "docker", "build", "--build-arg", "CMD=./cmd/gateway", "-t", "ws-migrator/gateway:latest", ".")
	sh(t, "docker", "build", "--build-arg", "CMD=./test/counter", "-t", "ws-migrator/worker:latest", ".")
	sh(t, kind, "load", "docker-image", "ws-migrator/gateway:latest", "ws-migrator/worker:latest", "--name", kindCluster)

	sh(t, "kubectl", "--context", kubeContext, "delete", "namespace", namespace, "--ignore-not-found", "--wait")
	sh(t, "kubectl", "--context", kubeContext, "create", "namespace", namespace)
	for _, file := range []string{"secret.example.yaml", "redis.yaml", "gateway.yaml", "worker.yaml"} {
		kubectl(t, "apply", "-f", "deploy/k8s/"+file)
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Log(kubectl(t, "get", "pods", "-o", "wide"))
			t.Log(kubectl(t, "logs", "deployment/ws-gateway", "--tail", "40"))
		}
	})

	for _, d := range []string{"ws-redis", "ws-gateway", "ws-worker"} {
		kubectl(t, "rollout", "status", "deployment/"+d, "--timeout", "180s")
	}

	kubectl(t, "scale", "deployment/ws-worker", fmt.Sprintf("--replicas=%d", k8sWorkers))
	kubectl(t, "rollout", "status", "deployment/ws-worker", "--timeout", "180s")

	portForward(t)
	waitWorkers(t, k8sGateway, k8sWorkers)

	f := openFleet(t, k8sGateway, k8sSessions)
	t.Logf("start: %v", f.placement())

	t.Run("rolling restart of every worker", func(t *testing.T) {
		before := workerPods(t)
		migrated := f.stats().MigrationsSucceeded

		kubectl(t, "rollout", "restart", "deployment/ws-worker")
		kubectl(t, "rollout", "status", "deployment/ws-worker", "--timeout", "600s")
		f.waitProgress("the rolling restart", 20)
		waitWorkers(t, k8sGateway, k8sWorkers)

		placed := f.placement()
		for _, old := range before {
			if placed[old] != 0 {
				t.Fatalf("%d sessions still on replaced pod %s", placed[old], old)
			}
		}
		s := f.stats()
		if s.MigrationsSucceeded == migrated {
			t.Fatal("the rolling restart migrated no sessions")
		}
		t.Logf("after rollout: %v, %d migrations", placed, s.MigrationsSucceeded-migrated)
	})

	t.Run("crashed container", func(t *testing.T) {
		placed := f.placement()
		victim, held := "", 0
		for pod, n := range placed {
			if n > held {
				victim, held = pod, n
			}
		}
		recovered := f.stats().RecoveriesSucceeded

		node := kindCluster + "-control-plane"
		id := strings.TrimSpace(sh(t, "docker", "exec", node, "crictl", "ps", "-q", "--label", "io.kubernetes.pod.name="+victim))
		sh(t, "docker", "exec", node, "crictl", "stop", "--timeout", "0", id)
		f.waitProgress("crashing "+victim, 20)

		if got := f.stats().RecoveriesSucceeded - recovered; got < uint64(held) {
			t.Fatalf("%d recoveries for %d sessions on the crashed pod", got, held)
		}
		kubectl(t, "rollout", "status", "deployment/ws-worker", "--timeout", "180s")
		waitWorkers(t, k8sGateway, k8sWorkers)
		t.Logf("after kill: %v", f.placement())
	})

	t.Run("scale up then down", func(t *testing.T) {
		kubectl(t, "scale", "deployment/ws-worker", "--replicas=9")
		kubectl(t, "rollout", "status", "deployment/ws-worker", "--timeout", "180s")
		waitWorkers(t, k8sGateway, 9)

		for pod := range f.placement() {
			kubectl(t, "annotate", "pod", pod, "--overwrite", "controller.kubernetes.io/pod-deletion-cost=-1")
		}
		migrated := f.stats().MigrationsSucceeded

		kubectl(t, "scale", "deployment/ws-worker", "--replicas=3")
		f.waitStats("scale-down to finish", func(s stats) bool { return s.WorkersTotal == 3 })
		f.waitProgress("scaling down", 20)

		alive := map[string]bool{}
		for _, pod := range workerPods(t) {
			alive[pod] = true
		}
		for pod, n := range f.placement() {
			if !alive[pod] {
				t.Fatalf("%d sessions on %s, which no longer runs", n, pod)
			}
		}
		if f.stats().MigrationsSucceeded == migrated {
			t.Fatal("the scale-down migrated no sessions")
		}
		t.Logf("after scale-down: %v", f.placement())
	})

	f.check("the whole run")
	s := f.stats()
	if s.ResendGaps != 0 || s.RecoveriesFailed != 0 {
		t.Fatalf("unexpected failures: %+v", s)
	}
	if int(s.SessionsActive) != len(f.sessions) {
		t.Fatalf("%d sessions active, wanted %d", s.SessionsActive, len(f.sessions))
	}
	t.Logf("final: %+v", s)
}
