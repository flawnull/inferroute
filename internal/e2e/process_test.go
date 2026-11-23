package e2e

// Black-box tests against the real binaries: cmd/router and cmd/worker are
// built once, started as separate OS processes on loopback ports, and driven
// only through their public HTTP APIs and Unix signals. Nothing is mocked; the
// GPU itself is the worker's built-in simulation. Skipped with -short.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"inferroute/internal/router"
)

var bins struct {
	once           sync.Once
	router, worker string
	err            error
}

func binaries(t *testing.T) (routerBin, workerBin string) {
	t.Helper()
	if testing.Short() {
		t.Skip("process tests build and run the real binaries; skipped with -short")
	}
	bins.once.Do(func() {
		dir, err := os.MkdirTemp("", "inferroute-bin-")
		if err != nil {
			bins.err = err
			return
		}
		bins.router, bins.worker = filepath.Join(dir, "router"), filepath.Join(dir, "worker")
		for _, cmd := range []*exec.Cmd{
			exec.CommandContext(context.Background(), "go", "build", "-race", "-o", bins.router, "inferroute/cmd/router"),
			exec.CommandContext(context.Background(), "go", "build", "-race", "-o", bins.worker, "inferroute/cmd/worker"),
		} {
			if b, err := cmd.CombinedOutput(); err != nil {
				bins.err = fmt.Errorf("%s: %w\n%s", cmd, err, b)
				return
			}
		}
	})
	if bins.err != nil {
		t.Fatal(bins.err)
	}
	return bins.router, bins.worker
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

// proc is one running binary. Its combined output is kept for failure reports.
type proc struct {
	name string
	cmd  *exec.Cmd
	out  *bytes.Buffer
	done chan struct{}
	err  error
}

func start(t *testing.T, name, bin string, env ...string) *proc {
	t.Helper()
	p := &proc{name: name, out: &bytes.Buffer{}, done: make(chan struct{})}
	p.cmd = exec.CommandContext(context.Background(), bin)
	p.cmd.Env = append(os.Environ(), append([]string{"LOG_LEVEL=warn"}, env...)...)
	var mu sync.Mutex
	w := writerFunc(func(b []byte) (int, error) { mu.Lock(); defer mu.Unlock(); return p.out.Write(b) })
	p.cmd.Stdout, p.cmd.Stderr = w, w
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { p.err = p.cmd.Wait(); close(p.done) }()
	t.Cleanup(func() {
		_ = p.cmd.Process.Kill()
		<-p.done
		if t.Failed() {
			t.Logf("--- %s output ---\n%s", p.name, p.out.String())
		}
	})
	return p
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(b []byte) (int, error) { return f(b) }

// wait returns the exit error, or fails the test if the process outlives d.
func (p *proc) wait(t *testing.T, d time.Duration) error {
	t.Helper()
	select {
	case <-p.done:
		return p.err
	case <-time.After(d):
		t.Fatalf("%s still running %v after the signal", p.name, d)
		return nil
	}
}

type node struct {
	*proc
	id, grpc, admin string
}

func startWorkerProc(t *testing.T, bin, id, grpcAddr string) *node {
	t.Helper()
	admin := freeAddr(t)
	p := start(t, id, bin,
		"WORKER_ID="+id, "GRPC_ADDR="+grpcAddr, "ADMIN_ADDR="+admin,
		"TOTAL_VRAM_MB=40000", "BASE_LATENCY_MS=5", "JITTER_MS=5", "PER_TOKEN_MS=0.1", "DRAIN_TIMEOUT=5")
	return &node{proc: p, id: id, grpc: grpcAddr, admin: admin}
}

type cluster struct {
	t       *testing.T
	url     string
	router  *proc
	workers []*node
	wbin    string
}

// newCluster starts n worker processes and one router process, and waits
// until the router reports every worker healthy.
func newCluster(t *testing.T, n int, routerEnv ...string) *cluster {
	t.Helper()
	rbin, wbin := binaries(t)
	c := &cluster{t: t, wbin: wbin}
	addrs := make([]string, n)
	for i := range n {
		c.workers = append(c.workers, startWorkerProc(t, wbin, fmt.Sprintf("worker-%d", i+1), freeAddr(t)))
		addrs[i] = c.workers[i].grpc
	}
	listen := freeAddr(t)
	c.url = "http://" + listen
	env := []string{
		"LISTEN_ADDR=" + listen, "WORKERS=" + strings.Join(addrs, ","),
		"MODELS=llama-3-8b=2048,phi-3-mini=1024",
		"HEARTBEAT_INTERVAL=50", "HEARTBEAT_TIMEOUT=40", "UNHEALTHY_AFTER=2",
		"BREAKER_FAILURE_THRESHOLD=3", "BREAKER_OPEN_TIMEOUT=200",
		"ATTEMPT_TIMEOUT=2000", "MAX_ATTEMPTS=2", "QUEUE_TIMEOUT=1000",
		"RATE_LIMIT_RPS=1000000", "RATE_LIMIT_BURST=1000000", "SHUTDOWN_TIMEOUT=5",
	}
	// later entries win in os/exec, so routerEnv overrides the defaults above
	c.router = start(t, "router", rbin, append(env, routerEnv...)...)
	c.waitWorkers(n)
	return c
}

func (c *cluster) health() (code int, body struct {
	HealthyWorkers int                   `json:"healthy_workers"`
	Workers        []router.WorkerStatus `json:"workers"`
}) {
	req, err := http.NewRequestWithContext(c.t.Context(), http.MethodGet, c.url+"/readyz", nil)
	if err != nil {
		return -1, body
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return -1, body
	}
	defer func() { _ = resp.Body.Close() }()
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return resp.StatusCode, body
}

func (c *cluster) waitWorkers(n int) {
	c.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, b := c.health(); b.HealthyWorkers == n {
			closed := 0
			for _, w := range b.Workers {
				if w.Healthy && w.Breaker == "closed" {
					closed++
				}
			}
			if closed == n {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	_, b := c.health()
	c.t.Fatalf("router never saw %d healthy workers: %+v", n, b)
}

func (c *cluster) worker(id string) router.WorkerStatus {
	_, b := c.health()
	for _, w := range b.Workers {
		if w.ID == id {
			return w
		}
	}
	return router.WorkerStatus{}
}

func (c *cluster) infer(client *http.Client, i int) result {
	model := []string{"llama-3-8b", "phi-3-mini"}[i%2]
	body := fmt.Sprintf(`{"model_id":%q,"prompt_tokens":32,"max_tokens":16}`, model)
	req, err := http.NewRequestWithContext(c.t.Context(), http.MethodPost, c.url+"/v1/infer", strings.NewReader(body))
	if err != nil {
		return result{code: -1}
	}
	req.Header.Set("X-Tenant-ID", fmt.Sprintf("tenant-%d", i%3))
	resp, err := client.Do(req)
	if err != nil {
		return result{code: -1}
	}
	defer func() { _ = resp.Body.Close() }()
	var out router.InferResponse
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return result{code: resp.StatusCode, workerID: out.WorkerID, attempts: out.Attempts}
}

// load runs concurrent clients until stop is called and returns every result
// in completion order.
func (c *cluster) load(concurrency int) (stop func() []result) {
	client := &http.Client{Timeout: 10 * time.Second}
	var mu sync.Mutex
	var out []result
	var seq atomic.Int64
	quit := make(chan struct{})
	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-quit:
					return
				default:
				}
				r := c.infer(client, int(seq.Add(1)))
				mu.Lock()
				out = append(out, r)
				mu.Unlock()
			}
		}()
	}
	return func() []result { close(quit); wg.Wait(); return out }
}

func (c *cluster) setFaults(n *node, body string) {
	c.t.Helper()
	c.admin(n, http.MethodPost, body)
}

// admin calls a worker's /admin/faults with method and requires 200.
func (c *cluster) admin(n *node, method, body string) {
	c.t.Helper()
	req, err := http.NewRequestWithContext(c.t.Context(), method, "http://"+n.admin+"/admin/faults", strings.NewReader(body))
	if err != nil {
		c.t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	if err := resp.Body.Close(); err != nil {
		c.t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		c.t.Fatalf("POST /admin/faults = %d", resp.StatusCode)
	}
}

func (c *cluster) metric(name string) float64 {
	c.t.Helper()
	req, err := http.NewRequestWithContext(c.t.Context(), http.MethodGet, c.url+"/metrics", nil)
	if err != nil {
		c.t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		c.t.Fatal(err)
	}
	var total float64
	for _, line := range strings.Split(buf.String(), "\n") {
		if strings.HasPrefix(line, name+"{") || strings.HasPrefix(line, name+" ") {
			fields := strings.Fields(line)
			v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
			if err != nil {
				c.t.Fatalf("metric line %q: %v", line, err)
			}
			total += v
		}
	}
	return total
}

func servedBy(rs []result, id string) int {
	n := 0
	for _, r := range rs {
		if r.code == 200 && r.workerID == id {
			n++
		}
	}
	return n
}

func TestProcesses(t *testing.T) {
	t.Run("requests are served and spread across real worker processes", func(t *testing.T) {
		c := newCluster(t, 2)
		client := &http.Client{Timeout: 5 * time.Second}
		var rs []result
		for i := range 40 {
			rs = append(rs, c.infer(client, i))
		}
		byWorker, _ := summarize(t, rs)
		if byWorker["worker-1"] == 0 || byWorker["worker-2"] == 0 {
			t.Fatalf("traffic not spread across both workers: %v", byWorker)
		}
	})

	t.Run("SIGKILL of a worker process under load drops no requests", func(t *testing.T) {
		c := newCluster(t, 2)
		stop := c.load(24)
		time.Sleep(400 * time.Millisecond)
		_ = c.workers[0].cmd.Process.Signal(syscall.SIGKILL)
		time.Sleep(800 * time.Millisecond)
		rs := stop()
		byWorker, _ := summarize(t, rs)
		if byWorker["worker-1"] == 0 {
			t.Fatal("worker-1 served nothing before it was killed; the test proved nothing")
		}
		if late := servedBy(rs[len(rs)*3/4:], "worker-2"); late == 0 {
			t.Fatal("worker-2 did not take over the traffic")
		}
	})

	t.Run("the router marks a killed worker unhealthy and stays ready on the survivor", func(t *testing.T) {
		c := newCluster(t, 2)
		stop := c.load(8)
		time.Sleep(200 * time.Millisecond)
		_ = c.workers[0].cmd.Process.Signal(syscall.SIGKILL)
		time.Sleep(600 * time.Millisecond)
		stop()
		if s := c.worker("worker-1"); s.Healthy {
			t.Fatalf("worker-1 still reported healthy: %+v", s)
		}
		if code, _ := c.health(); code != 200 {
			t.Fatalf("/readyz = %d with one healthy worker left, want 200", code)
		}
	})

	t.Run("failovers caused by a crash are counted in /metrics", func(t *testing.T) {
		c := newCluster(t, 2)
		stop := c.load(24)
		time.Sleep(300 * time.Millisecond)
		_ = c.workers[0].cmd.Process.Signal(syscall.SIGKILL)
		time.Sleep(500 * time.Millisecond)
		rs := stop()
		_, retried := summarize(t, rs)
		got := c.metric("inferroute_failovers_total")
		if retried > 0 && got == 0 {
			t.Fatalf("%d requests were retried but inferroute_failovers_total is 0", retried)
		}
		if got == 0 {
			t.Skip("the crash landed between requests, so no request needed a failover")
		}
	})

	t.Run("SIGTERM drains a worker: nothing is dropped and the process exits 0", func(t *testing.T) {
		c := newCluster(t, 2)
		stop := c.load(16)
		time.Sleep(300 * time.Millisecond)
		_ = c.workers[0].cmd.Process.Signal(syscall.SIGTERM)
		err := c.workers[0].wait(t, 10*time.Second)
		time.Sleep(300 * time.Millisecond)
		rs := stop()
		summarize(t, rs)
		if err != nil {
			t.Fatalf("drained worker exited with %v, want status 0", err)
		}
	})

	t.Run("an error storm injected over the admin API moves traffic to the healthy worker", func(t *testing.T) {
		c := newCluster(t, 2)
		c.setFaults(c.workers[0], `{"error_rate":1}`)
		stop := c.load(12)
		time.Sleep(800 * time.Millisecond)
		rs := stop()
		summarize(t, rs)
		if n := servedBy(rs, "worker-1"); n != 0 {
			t.Fatalf("worker-1 answered %d requests while failing every one", n)
		}
		if s := c.worker("worker-1"); s.Breaker == "closed" {
			t.Fatalf("worker-1 breaker still closed under a 100%% error rate: %+v", s)
		}
	})

	t.Run("a hung worker process is abandoned after ATTEMPT_TIMEOUT and the request still succeeds", func(t *testing.T) {
		c := newCluster(t, 2, "ATTEMPT_TIMEOUT=300")
		c.setFaults(c.workers[0], `{"extra_latency_ms":60000}`)
		client := &http.Client{Timeout: 5 * time.Second}
		var rs []result
		for i := range 6 {
			start := time.Now()
			r := c.infer(client, i)
			if el := time.Since(start); el > 2*time.Second {
				t.Fatalf("request %d took %v; the hung attempt should have been cut at 300ms", i, el)
			}
			rs = append(rs, r)
		}
		summarize(t, rs)
		if n := servedBy(rs, "worker-1"); n != 0 {
			t.Fatalf("the hung worker answered %d requests", n)
		}
	})

	t.Run("a request whose deadline passes while every worker hangs gets 504", func(t *testing.T) {
		c := newCluster(t, 1)
		c.setFaults(c.workers[0], `{"extra_latency_ms":60000}`)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, c.url+"/v1/infer", strings.NewReader(`{"model_id":"phi-3-mini","max_tokens":4,"timeout_ms":200}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-Tenant-ID", "t")
		start := time.Now()
		resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if err := resp.Body.Close(); err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusGatewayTimeout {
			t.Fatalf("status %d, want 504", resp.StatusCode)
		}
		if el := time.Since(start); el > time.Second {
			t.Fatalf("504 took %v, want about the 200ms deadline", el)
		}
	})

	t.Run("a worker whose faults are cleared earns its traffic back", func(t *testing.T) {
		c := newCluster(t, 2)
		c.setFaults(c.workers[0], `{"error_rate":1}`)
		stop := c.load(8)
		time.Sleep(500 * time.Millisecond)
		c.admin(c.workers[0], http.MethodDelete, "")
		c.waitWorkers(2)
		time.Sleep(400 * time.Millisecond)
		rs := stop()
		summarize(t, rs)
		if servedBy(rs[len(rs)/2:], "worker-1") == 0 {
			t.Fatal("worker-1 got no traffic after its faults were cleared")
		}
	})

	t.Run("a crashed worker restarted on the same address rejoins and gets traffic", func(t *testing.T) {
		c := newCluster(t, 2)
		stop := c.load(12)
		time.Sleep(200 * time.Millisecond)
		dead := c.workers[0]
		_ = dead.cmd.Process.Signal(syscall.SIGKILL)
		<-dead.done
		time.Sleep(400 * time.Millisecond)
		startWorkerProc(t, c.wbin, "worker-1", dead.grpc)
		c.waitWorkers(2)
		time.Sleep(400 * time.Millisecond)
		rs := stop()
		summarize(t, rs)
		if servedBy(rs[len(rs)/2:], "worker-1") == 0 {
			t.Fatal("restarted worker-1 received no traffic")
		}
	})

	t.Run("with every worker down the router answers 503 and readiness fails", func(t *testing.T) {
		c := newCluster(t, 1)
		_ = c.workers[0].cmd.Process.Signal(syscall.SIGKILL)
		<-c.workers[0].done
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if code, _ := c.health(); code == http.StatusServiceUnavailable {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if code, _ := c.health(); code != http.StatusServiceUnavailable {
			t.Fatalf("/readyz = %d with no workers, want 503", code)
		}
		if r := c.infer(&http.Client{Timeout: 5 * time.Second}, 0); r.code != http.StatusServiceUnavailable {
			t.Fatalf("infer = %d with no workers, want 503", r.code)
		}
	})

	t.Run("the router exits 0 on SIGTERM", func(t *testing.T) {
		c := newCluster(t, 1)
		_ = c.router.cmd.Process.Signal(syscall.SIGTERM)
		if err := c.router.wait(t, 10*time.Second); err != nil {
			t.Fatalf("router exited with %v, want status 0", err)
		}
	})
}
