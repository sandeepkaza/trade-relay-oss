// k6 smoke test — hits the app's health endpoint under light load.
// Used by the CI `smoke` stage against a freshly built container.
// Report-only there: catches a dead / 5xx service, not perf regressions.
import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE = __ENV.SMOKE_BASE_URL || 'http://localhost:8000';

export const options = {
  vus: 2,
  duration: '20s',
  thresholds: {
    http_req_failed: ['rate<0.01'],   // <1% errors
    http_req_duration: ['p(95)<500'], // 95th pct under 500ms
  },
};

export default function () {
  const res = http.get(`${BASE}/api/health`);
  check(res, {
    'status is 200': (r) => r.status === 200,
    'body non-empty': (r) => r.body && r.body.length > 0,
  });
  sleep(1);
}
