# inferroute

[![ci](https://github.com/flawnull/inferroute/actions/workflows/ci.yml/badge.svg)](https://github.com/flawnull/inferroute/actions/workflows/ci.yml)

A distributed inference router and scheduler in Go. It routes simulated LLM inference requests across a pool of GPU workers. Placement accounts for VRAM. It enforces per-tenant token-bucket rate limits, runs a circuit breaker per worker, fails over automatically, and exports Prometheus metrics.

Workers simulate GPUs: each one has a VRAM budget, a latency profile, batch contention, and fault injection. The control plane around them is built the way you would build one for real hardware.

## Architecture

```
                 HTTP/JSON                                      gRPC (proto3)
 clients ──── POST /v1/infer ────►┌──────────────────────────────┐  Infer()      ┌─────────────────────┐
 (X-Tenant-ID)                    │            router            │──────────────►│ worker-1  24 GB     │
                                  │                              │◄── Heartbeat ─│ fast                │
                                  │ 1. token bucket per tenant   │   every 500ms └─────────────────────┘
                                  │    (429 + Retry-After)       │               ┌─────────────────────┐
                                  │ 2. validate, resolve VRAM    │──────────────►│ worker-2  48 GB     │
                                  │ 3. scheduler: score healthy, │◄──────────────│ medium              │
                                  │    breaker-ready workers,    │               └─────────────────────┘
                                  │    CAS-reserve VRAM          │               ┌─────────────────────┐
                                  │ 4. Infer; on Unavailable /   │──────────────►│ worker-3  80 GB     │
                                  │    ResourceExhausted /       │◄──────────────│ large, slow         │
                                  │    timeout: retry once on a  │               └─────────────────────┘
                                  │    different worker          │                each worker: VRAM pool,
                                  │                              │                drain on SIGTERM,
                                  │ registry: heartbeats, health,│                /admin/faults, /metrics
                                  │ breakers, identity pinning   │
                                  └──────────────┬───────────────┘
                                                 │ /metrics
                                  ┌──────────────▼───────────────┐     ┌─────────────────────┐
                                  │ Prometheus (scrapes router   │────►│ Grafana (provisioned│
                                  │ and all workers every 5s)    │     │ dashboard)          │
                                  └──────────────────────────────┘     └─────────────────────┘
```

| Path | Contents |
|---|---|
| `proto/inference.proto` | `WorkerService` with `Infer` and `Heartbeat` |
| `gen/inferencepb` | Generated Go stubs (committed, so builds need no `protoc`) |
| `internal/worker` | Simulated worker: atomic VRAM pool, latency model, fault injection, drain |
| `internal/router` | Registry, scheduler, rate limiter, circuit breaker, failover, HTTP API, metrics |
| `internal/e2e` | In-process router plus real gRPC workers: crash, drain, restart, duplicate-identity tests |
| `cmd/router`, `cmd/worker` | Binaries, configured through environment variables |
| `deploy/` | Prometheus config and Grafana provisioning |
| `k8s/` | Kustomize manifests for Kind: Deployments, headless Service, HPA, PDB |
| `benchmarks/` | `load.js` (k6) and `chaos.sh` |

## Quickstart

Requirements: Go 1.26+, Docker with Compose v2. `protoc` is only needed if you change the proto.

```bash
make test-race
```

```bash
make up
```

```bash
curl -s localhost:8080/v1/infer -H 'X-Tenant-ID: tenant-pro' \
  -d '{"model_id":"llama-3-8b","prompt_tokens":128,"max_tokens":64}'
```

```bash
make bench
```

```bash
make chaos-bench
```

```bash
make down
```

- Grafana: http://localhost:3000 (anonymous viewer; admin/admin). The inferroute dashboard is the home page.
- Prometheus: http://localhost:9090
- Router health with the per-worker view: `curl localhost:8080/healthz`
- Worker admin ports: `localhost:9101`, `9102`, `9103`

Inject faults into a running worker:

```bash
curl -X POST localhost:9102/admin/faults -d '{"error_rate":0.3}'
```

```bash
curl -X POST localhost:9102/admin/faults -d '{"slowdown":4,"extra_latency_ms":200}'
```

```bash
curl -X DELETE localhost:9102/admin/faults
```

The same knobs exist as env vars: `FAULT_ERROR_RATE`, `FAULT_EXTRA_LATENCY_MS`, `FAULT_SLOWDOWN`, `FAULT_UNHEALTHY`.

### Kind

```bash
make kind-up
```

```bash
make kind-metrics-server
```

```bash
make kind-deploy
```

```bash
curl -s localhost:8081/v1/infer -H 'X-Tenant-ID: tenant-pro' -d '{"model_id":"phi-3-mini","max_tokens":32}'
```

The router discovers workers through the headless Service `inferroute-worker`. It re-resolves that name every 5s, so pods added by the HPA join the pool without a restart. A draining worker fails its readiness probe, which drops it from DNS, and it reports `healthy=false` in heartbeats at the same moment.

## API

`POST /v1/infer`. The `X-Tenant-ID` header is required. `X-Request-ID` is optional and echoed back.

```json
{"model_id": "llama-3-8b", "prompt_tokens": 128, "max_tokens": 64,
 "required_vram_mb": 3000, "timeout_ms": 5000}
```

`required_vram_mb` overrides the model catalog (`MODELS`). `timeout_ms` is capped by `MAX_REQUEST_TIMEOUT`.

| Status | Meaning |
|---|---|
| 200 | `{request_id, worker_id, output, completion_tokens, compute_ms, latency_ms, attempts}` |
| 400 | Missing tenant, bad JSON, unknown model without `required_vram_mb` |
| 429 | Tenant over its rate limit. `Retry-After` is the seconds until the next token. |
| 503 | No healthy worker, no worker large enough, or no VRAM freed within `QUEUE_TIMEOUT`. `Retry-After: 1`. |
| 504 | Request deadline exceeded |

`GET /healthz` is liveness and always returns 200 with per-worker state. `GET /readyz` returns 503 until at least one worker is healthy. `GET /metrics` serves Prometheus metrics.

## Design and tradeoffs

### gRPC between router and workers, REST at the edge

The router talks to workers over gRPC. The schema is typed and shared with codegen. Calls are multiplexed over one long-lived HTTP/2 connection per worker, so there is no connection churn under load. Deadlines propagate on the wire, and status codes (`Unavailable`, `ResourceExhausted`, `DeadlineExceeded`) map directly onto retry and breaker decisions. Heartbeats reuse the same connection, so a broken heartbeat means the data path is broken too.

The public API stays HTTP/JSON because clients expect curl-ability, k6 speaks it natively, and per-tenant headers and `Retry-After` are HTTP concepts. The cost is a translation layer in the router, and you can't stream tokens end to end without adding SSE.

### Scheduling

A request is eligible on a worker that is healthy, whose breaker admits it, and whose VRAM can fit it. Among those, the lowest score wins:

```
score = ewma_latency_ms × (1 + queue_depth) × (1 + vram_utilisation_after_placement)
```

The first two factors estimate time to completion on a worker that serves its queue with contention. The third spreads memory pressure so large requests (`mixtral-8x7b`, 6 GB) keep finding room. Spreading trades bin-packing density for headroom; a bin-packing variant would score the opposite way. `queue_depth` is the larger of the router's in-flight count, which updates instantly, and the worker's reported count, which covers other routers. Scoring sorts all candidates, which is fine for tens of workers. Hundreds of workers would want power-of-two-choices sampling.

When every eligible worker is momentarily full, the request waits up to `QUEUE_TIMEOUT` and wakes on any lease release or heartbeat. It does not poll. A request that no worker could ever fit fails immediately.

### Optimistic VRAM reservation

The router keeps a control-plane reservation counter per worker and claims VRAM with a CAS loop before sending the RPC. A burst of concurrent requests can't oversubscribe the router's view: the scheduler tests grant exactly `floor(capacity / size)` leases across 200 racing goroutines. The worker enforces the real limit with its own atomic pool and returns `ResourceExhausted`, which the router treats as "retry elsewhere" and not as a health failure.

Heartbeats reconcile the two views. The worker reports total usage, including this router's in-flight requests. The router subtracts the larger of its reservation snapshots taken before and after the RPC, and attributes the rest to other routers. Choosing the larger snapshot keeps the estimate optimistic. The alternative, a pessimistic estimate, would leave phantom usage after every busy heartbeat and strand capacity. With optimism the occasional overshoot costs one retry.

### Circuit breaker without a thundering herd

Each worker has a breaker. Closed trips to Open after `BREAKER_FAILURE_THRESHOLD` consecutive failures. Only health signals count as failures: `Unavailable`, attempt timeouts, internal errors. Capacity rejections and client cancellations are neutral. Three mechanisms keep recovery from turning into a stampede:

1. **Jittered open period.** Each trip waits `OPEN_TIMEOUT × (1 + rand × BREAKER_JITTER)`, doubling on consecutive trips up to `BREAKER_MAX_OPEN_TIMEOUT`. Breakers that tripped together, across workers or across router replicas, do not probe together.
2. **Bounded half-open probes.** Half-open admits at most `BREAKER_HALF_OPEN_PROBES` concurrent requests. It closes after `BREAKER_HALF_OPEN_SUCCESSES` successes and re-opens on the first failure. A recovering worker gets a trickle, then full traffic.
3. **Generation-tagged tickets.** Every admission carries the breaker generation. Results from requests admitted before a state change are ignored, so a slow success from before a crash cannot close a breaker.

Heartbeat failure also trips the breaker, once, on the healthy-to-unhealthy edge. While the worker stays down the breaker drifts to half-open and waits. When heartbeats return, real traffic probes it right away and no backoff has piled up. In the chaos runs the restarted worker took 0.5 to 2.1s to go from container start to a closed breaker.

### Retry and failover

A failed attempt is retried once (`MAX_ATTEMPTS=2`) on a different worker, inside the request's deadline. Retryable outcomes are `Unavailable` (crash, drain, injected fault), `ResourceExhausted`, and an attempt timeout. Every attempt except the last is capped at `ATTEMPT_TIMEOUT`, so a worker that hangs, as opposed to one that errors, still leaves time to fail over. The cost is that a legitimately slow request over `ATTEMPT_TIMEOUT` is cancelled and re-run. Set `ATTEMPT_TIMEOUT` above your slowest expected request.

Retries are not hedged. A hedged request would cut tail latency but double GPU work for every slow request, and GPU time is the scarce resource here.

### Identity pinning

Containers that are recreated and pods whose IPs get recycled can leave a router connection pointing at the wrong process. This showed up during development: after `docker compose up` recreated all workers, `worker-3:50051` was answering as `worker-2`. The router then counted worker-2's VRAM twice and `/metrics` failed on duplicate series. The registry now pins each address to the first `worker_id` it reports. A changed ID, or an ID another address already owns, makes the router drop the connection and redial with a fresh DNS lookup. `TestDuplicateIdentityIsRejected` covers it.

The gRPC client also uses the `passthrough` resolver so every reconnect resolves DNS. The default `dns` resolver rate-limits re-resolution to once per 30s, and the default reconnect backoff tops out at 120s. Both are lowered so a restarted worker is reachable within about 2s.

### Rate limiting

There is one token bucket per tenant, refilled lazily from elapsed time with no background tickers. Buckets sit in a map under an RWMutex with a mutex per bucket, so tenants don't contend with each other. Idle buckets that have refilled completely are evicted every minute, which bounds memory without changing behavior. `Retry-After` is the time until one token refills, rounded up to whole seconds.

### Scaling the router

Rate limits and VRAM reservations live in each router process, which is why `k8s/router.yaml` runs one replica. With N replicas, each tenant effectively gets N times its limit, unless you divide the configured rates by N or move the buckets to a shared store such as Redis with a Lua script. VRAM accounting already tolerates several routers: heartbeats attribute other routers' usage to `externalUsed`, and the worker's `ResourceExhausted` is the backstop. Expect more capacity retries as replicas are added.

### Graceful shutdown

On SIGTERM a worker stops accepting new RPCs and returns `Unavailable`, which the router retries elsewhere. It reports `healthy=false` in heartbeats, fails readiness, and waits up to `DRAIN_TIMEOUT` for in-flight requests before `GracefulStop`. The router stops accepting connections, finishes in-flight requests while its heartbeat loop keeps running so they can still fail over, then exits.

## Metrics

Router:

| Metric | Labels |
|---|---|
| `inferroute_requests_total` | `status`, `model`, `tenant` |
| `inferroute_request_duration_seconds` (histogram) | `model`, `status` |
| `inferroute_failovers_total` | `from_worker`, `reason` |
| `inferroute_worker_attempts_total` | `worker`, `code` |
| `inferroute_worker_vram_used_mb`, `inferroute_worker_vram_total_mb` | `worker` |
| `inferroute_circuit_breaker_state` (0 closed, 1 half-open, 2 open) | `worker` |
| `inferroute_worker_healthy`, `inferroute_worker_inflight`, `inferroute_worker_ewma_latency_ms` | `worker` |

Workers export `inferworker_*` metrics: requests by gRPC code, compute histogram, VRAM used and total, active requests. The dashboard sets them next to the router's view so you can compare the router's estimate with each worker's actual usage.

The `model` label is limited to catalog models, with everything else counted as `other`. The `tenant` label is unbounded. If tenants are user-controlled, replace it with a tier label before production.

## Benchmark results

Measured on this repo's `docker compose` stack: 1 router and 3 workers on one Docker host, k6 1.0 in a container on the same network. The load is 200 req/s, constant arrival rate, for 60s. Traffic is 40% tenant-enterprise, 40% tenant-pro, and 20% tenant-free. The free tier is deliberately driven at 40 req/s against a 20 req/s limit, so about 10% of requests get a 429. Error rates count 5xx and transport failures only.

Steady state (`make bench`):

| Metric | Value |
|---|---|
| Throughput | 199.1 req/s |
| Latency p50 / p95 / p99 (200s) | 132 / 236 / 270 ms |
| 5xx error rate | 0.000% |
| 429 share | 9.35% |

Chaos (`make chaos-bench`): worker-1 SIGKILLed 15s into the load, restarted 20s later. These are three consecutive runs after the identity-pinning fix. Run 3 is on the final code; runs 1 and 2 predate refactors that did not change routing behavior.

| Metric | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| Throughput | 199.3 req/s | 199.3 req/s | 199.2 req/s |
| Latency p50 / p95 / p99 (200s) | 153 / 270 / 313 ms | 155 / 273 / 316 ms | 157 / 272 / 313 ms |
| Failovers | 9 | 8 | 12 |
| 5xx during outage window | 0 / 3992 | 0 / 3990 | 0 / 3992 |
| 5xx error rate, whole run | 0.000% | 0.000% | 0.000% |
| Detection (kill to unhealthy) | 797 ms | 1167 ms | 848 ms |
| Recovery (restart to breaker closed) | 470 ms | 1837 ms | 2103 ms |

Detection is bounded by `HEARTBEAT_INTERVAL × UNHEALTHY_AFTER`, here 500ms × 2. Requests caught in flight on the killed worker, or placed on it before detection, are the failovers, and every one of them succeeded on the retry. Recovery includes container start, the next heartbeat, and two successful half-open probes. Its spread comes from where the breaker's jittered timer sits when the worker returns.

**Capacity matters more than failover logic.** In an earlier configuration the workers simulated 5% latency contention per concurrent request. With worker-1 gone, the two survivors saturated at about 175 req/s, right at the offered load. The router then shed 33% of requests during the outage with `503 timed out waiting for VRAM` and never oversubscribed a GPU. Of the 3,313 failed requests the router logged in that run, 4 involved the failover path; the rest were queue timeouts. The current profile (2% contention) is provisioned N+1. Rerun with `RATE=300 ./benchmarks/chaos.sh` to see load shedding.

Results land in `benchmarks/results/`: a Markdown report, the k6 log, and a JSON summary.

## Configuration

Router (all optional):

| Env | Default | |
|---|---|---|
| `LISTEN_ADDR` | `:8080` | |
| `WORKERS` | `localhost:50051` | Static `host:port` list |
| `WORKER_DNS` | | `host:port` resolved every `DISCOVERY_INTERVAL`; overrides `WORKERS` |
| `MODELS` | `phi-3-mini=1024,llama-3-8b=2048,mistral-7b=2048,mixtral-8x7b=6144` | Model to VRAM MB |
| `RATE_LIMIT_RPS`, `RATE_LIMIT_BURST` | `100`, `200` | Default per-tenant bucket |
| `MAX_TENANT_LABELS` | `100` | Distinct tenants that get their own metric label; later ones are reported as `other`. `X-Tenant-ID` is caller-controlled, so this bounds Prometheus cardinality |
| `TENANT_LIMITS` | | `tenant=rps:burst,...` |
| `HEARTBEAT_INTERVAL`, `HEARTBEAT_TIMEOUT`, `UNHEALTHY_AFTER` | `500ms`, `400ms`, `2` | |
| `BREAKER_FAILURE_THRESHOLD`, `BREAKER_OPEN_TIMEOUT`, `BREAKER_MAX_OPEN_TIMEOUT`, `BREAKER_JITTER` | `5`, `2s`, `30s`, `0.5` | |
| `BREAKER_HALF_OPEN_PROBES`, `BREAKER_HALF_OPEN_SUCCESSES` | `1`, `2` | |
| `REQUEST_TIMEOUT`, `MAX_REQUEST_TIMEOUT`, `ATTEMPT_TIMEOUT`, `MAX_ATTEMPTS`, `QUEUE_TIMEOUT` | `10s`, `60s`, `3s`, `2`, `1s` | |

Worker: `WORKER_ID`, `GRPC_ADDR` (`:50051`), `ADMIN_ADDR` (`:9091`), `TOTAL_VRAM_MB`, `BASE_LATENCY_MS`, `JITTER_MS`, `PER_TOKEN_MS`, `CONTENTION`, `DRAIN_TIMEOUT`, and the `FAULT_*` variables. Durations accept Go syntax (`250ms`) or a bare number in the documented unit. `LOG_LEVEL` sets the slog level for both binaries.

## Tests

`make test-race` runs `go test -race -count=1 ./...`:

- **Breaker:** threshold trips, neutral outcomes, half-open probe cap, exponential backoff with cap, jitter range and spread, stale tickets, concurrent use.
- **Rate limiter:** burst and deny with exact `Retry-After`, lazy refill, tenant isolation and overrides, exactly `burst` admits across 50 racing goroutines, eviction.
- **Scheduler:** latency preference, load balancing, filtering out unhealthy, open, too-small or full workers, exclusion, no-oversubscription under a 200-goroutine burst, waiting for released capacity, half-open single probe.
- **Worker:** VRAM CAS pool, `ResourceExhausted`, release on cancel and fault, drain semantics and timeout, admin endpoint.
- **Routing** (`internal/router/routing_test.go`), through the real HTTP handler, scheduler and breaker:
  - `Unavailable`, `ResourceExhausted` and a hung worker fail over to the next worker; the client sees one success.
  - `InvalidArgument` returns 400 and `Internal` returns 502, and neither is retried on another worker.
  - Failover stops at `MAX_ATTEMPTS`; an exhausted request gets 503 with `Retry-After`, an expired one 504.
  - Client errors, a caller's own deadline and a client disconnect never count against a worker's breaker.
  - `X-Request-ID` is echoed and forwarded, `timeout_ms` is capped, bodies over 1 MiB never reach a worker.
  - Caller-chosen tenant ids cannot create unbounded Prometheus series (see `MAX_TENANT_LABELS`).
- **In-process end to end** (`internal/e2e/failover_test.go`), with a real HTTP router and real gRPC workers on loopback:
  - Crash one of two workers under 24-way concurrent load: zero dropped requests.
  - Graceful drain under load: zero dropped requests.
  - Crash and restart on the same port: recovery through half-open and traffic resumes.
  - Two addresses for one worker: counted once, `/metrics` stays valid.
- **Process end to end** (`internal/e2e/process_test.go`): the real `router` and `worker` binaries, built with `-race`, run as separate OS processes and are driven only through HTTP and Unix signals:
  - `SIGKILL` of a worker under load drops no requests, and the failovers show up in `/metrics`.
  - `SIGTERM` drains a worker with nothing dropped, and the process exits 0.
  - An error storm injected through `/admin/faults` opens the worker's breaker and moves all traffic away; clearing it earns the traffic back.
  - A hung worker (60 s injected latency) is abandoned after `ATTEMPT_TIMEOUT`; with every worker hung, the caller gets 504 at its own deadline.
  - A crashed worker restarted on the same address rejoins; with no workers, `/readyz` and `/v1/infer` return 503.
  - The router exits 0 on `SIGTERM`.

`go test -short ./...` skips the process tests.

The tests were mutation-checked: each routing rule above was broken on purpose (no retry on `Unavailable`, no per-attempt timeout, no timeout cap, reusing the failed worker, one attempt too many, wrong status mapping, ignored `X-Request-ID` or `required_vram_mb`, a draining worker accepting work, an unbounded tenant label, and others), and a named test failed every time.

What is faked: the GPU itself, by design. In `routing_test.go` the workers are scripted gRPC clients, so those tests can return errors a real worker can't be made to produce (`Internal`, and `InvalidArgument` or `ResourceExhausted` after the router's own checks pass). The failover cases that a real worker can produce, `Unavailable` and a hang, are also covered against real worker processes in `process_test.go`.

## Not verified here

- `make kind-deploy` was not run end to end. The build sandbox's nested Docker could not start Kind's kubelet (cgroup v1 tmpfs). The manifests render with `kubectl kustomize` and pass `kubeconform -strict` against Kubernetes 1.32, and the images run as UID 65532 to satisfy `runAsNonRoot`.
- Benchmark numbers come from a single shared host. Expect different absolute latency on yours; the relative behavior (zero 5xx with N+1 capacity, sub-second detection) is what the benchmark is for.

## Building behind a TLS-intercepting proxy

The Dockerfiles accept an optional build secret named `ca`. Pass it with `docker build --secret id=ca,src=/path/to/ca-bundle.crt`, or under `build.secrets` in a Compose override. Without the secret, builds behave normally.
