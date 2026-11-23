// k6 load test for inferroute.
//
//   docker compose --profile bench run --rm k6 run /bench/load.js
//   k6 run -e BASE_URL=http://localhost:8080 -e RATE=300 -e DURATION=2m benchmarks/load.js
//
// Env: BASE_URL, RATE (req/s), DURATION, MAX_VUS, SUMMARY_FILE.
import http from 'k6/http';
import { check } from 'k6';
import { Rate, Trend, Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const RATE = parseInt(__ENV.RATE || '200', 10);
const DURATION = __ENV.DURATION || '60s';
const MAX_VUS = parseInt(__ENV.MAX_VUS || '400', 10);
const SUMMARY_FILE = __ENV.SUMMARY_FILE || '';

// Weighted mixes. tenant-free is deliberately pushed past its 20 req/s limit
// so the run exercises 429s alongside normal traffic.
const TENANTS = [
  ['tenant-enterprise', 0.4],
  ['tenant-pro', 0.4],
  ['tenant-free', 0.2],
];
const MODELS = [
  ['phi-3-mini', 0.35],
  ['llama-3-8b', 0.35],
  ['mistral-7b', 0.2],
  ['mixtral-8x7b', 0.1],
];

const serverErrors = new Rate('server_errors'); // 5xx or transport failure
const rateLimited = new Rate('rate_limited'); // 429
const okLatency = new Trend('latency_ok', true); // 200s only
const failovers = new Counter('failover_responses'); // 200 that needed >1 attempt

export const options = {
  discardResponseBodies: false,
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    inference: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: Math.min(MAX_VUS, Math.max(50, RATE)),
      maxVUs: MAX_VUS,
    },
  },
  thresholds: {
    server_errors: ['rate<0.01'],
    latency_ok: ['p(99)<2000'],
  },
};

function pick(weighted) {
  let r = Math.random();
  for (const [v, w] of weighted) {
    if ((r -= w) <= 0) return v;
  }
  return weighted[weighted.length - 1][0];
}

export default function () {
  const tenant = pick(TENANTS);
  const body = JSON.stringify({
    model_id: pick(MODELS),
    prompt_tokens: 32 + Math.floor(Math.random() * 480),
    max_tokens: 32 + Math.floor(Math.random() * 224),
  });
  const res = http.post(`${BASE_URL}/v1/infer`, body, {
    headers: { 'Content-Type': 'application/json', 'X-Tenant-ID': tenant },
    timeout: '15s',
    tags: { tenant },
  });

  const is5xx = res.status === 0 || res.status >= 500;
  serverErrors.add(is5xx);
  rateLimited.add(res.status === 429);
  if (res.status === 200) {
    okLatency.add(res.timings.duration);
    const attempts = res.json('attempts');
    if (attempts > 1) failovers.add(1);
  }
  check(res, {
    'not 5xx': () => !is5xx,
    '429 has Retry-After': (r) => r.status !== 429 || !!r.headers['Retry-After'],
  });
}

function num(m, key, d = 2) {
  const v = m && m.values ? m.values[key] : undefined;
  return typeof v === 'number' ? Number(v.toFixed(d)) : null;
}

export function handleSummary(data) {
  const m = data.metrics;
  const out = {
    duration_s: Number((data.state.testRunDurationMs / 1000).toFixed(1)),
    target_rps: RATE,
    achieved_rps: num(m.http_reqs, 'rate'),
    requests: m.http_reqs ? m.http_reqs.values.count : 0,
    p50_ms: num(m.latency_ok, 'med'),
    p95_ms: num(m.latency_ok, 'p(95)'),
    p99_ms: num(m.latency_ok, 'p(99)'),
    max_ms: num(m.latency_ok, 'max'),
    error_rate: num(m.server_errors, 'rate', 5),
    rate_limited_rate: num(m.rate_limited, 'rate', 4),
    failover_responses: m.failover_responses ? m.failover_responses.values.count : 0,
    dropped_iterations: m.dropped_iterations ? m.dropped_iterations.values.count : 0,
  };
  const lines = [
    '',
    'inferroute load summary',
    '-----------------------',
    `duration            ${out.duration_s}s`,
    `throughput          ${out.achieved_rps} req/s (target ${out.target_rps})`,
    `requests            ${out.requests}`,
    `latency (200s)      p50 ${out.p50_ms}ms  p95 ${out.p95_ms}ms  p99 ${out.p99_ms}ms  max ${out.max_ms}ms`,
    `error rate (5xx)    ${(out.error_rate * 100).toFixed(3)}%`,
    `rate limited (429)  ${(out.rate_limited_rate * 100).toFixed(2)}%`,
    `failover responses  ${out.failover_responses}`,
    `dropped iterations  ${out.dropped_iterations}`,
    '',
  ];
  const files = { stdout: lines.join('\n') };
  if (SUMMARY_FILE) files[SUMMARY_FILE] = JSON.stringify(out, null, 2);
  return files;
}
