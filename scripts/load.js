import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";

const ENTERPRISE_KEY = "enterprise-tenant-api-key-plaintext";
const PRO_KEY = "pro-tenant-api-key-plaintext";
const FREE_KEY = "free-tenant-api-key-plaintext";

const BASE_URL = "http://localhost:8080" // service port

const enterpriseRateLimited = new Counter("enterprise_rate_limited");
const proRateLimited = new Counter("pro_rate_limited");
const freeRateLimited = new Counter("free_rate_limited");
const freeBurstRateLimited = new Counter("free_burst_rate_limited");

const jobAccepted = new Rate("job_accepted");

// Latency under mixed load traffic
const jobEndToEndLoadMs = new Trend("job_end_to_end_load_ms", true);
const queueLatencyLoadMs = new Trend("queue_latency_load_ms", true);

// Latency under fairness contention
const jobEndToEndFairnessMs = new Trend("job_end_to_end_fairness_ms", true);
const queueLatencyFairnessMs = new Trend("queue_latency_fairness_ms", true);

const fairnessFreeRatio = new Rate("fairness_free_ratio");

export const options = {
  scenarios: {
    enterprise_load: {
      executor: "constant-arrival-rate",
      rate: 50, // test throughput under rate limit
      timeUnit: "1s",
      duration: "60s",
      preAllocatedVUs: 120,
      maxVUs: 500,
      startTime: "0s",
      exec: "submitEnterprise",
    },
    pro_load: {
      executor: "constant-arrival-rate",
      rate: 15, // test throughput under rate limit
      timeUnit: "1s",
      duration: "60s",
      preAllocatedVUs: 15,
      maxVUs: 60,
      startTime: "0s",
      exec: "submitPro",
    },
    free_load: {
      executor: "constant-arrival-rate",
      rate: 3, // expect no throttle
      timeUnit: "1s",
      duration: "60s",
      preAllocatedVUs: 3,
      maxVUs: 30,
      startTime: "0s",
      exec: "submitFree",
    },
    free_burst: {
      executor: "constant-arrival-rate",
      rate: 8, // should trigger free rate limit on burst
      timeUnit: "1s",
      duration: "10s",
      preAllocatedVUs: 5,
      maxVUs: 15,
      startTime: "25s",
      exec: "submitFreeBurst",
    },

    enterprise_fairness: {
      executor: "constant-arrival-rate",
      rate: 15,
      timeUnit: "1s",
      duration: "90s",
      preAllocatedVUs: 50,
      maxVUs: 200,
      startTime: "70s",
      exec: "submitEnterpriseFairness",
    },
    free_fairness: {
      executor: "constant-arrival-rate",
      rate: 5,
      timeUnit: "1s",
      duration: "90s",
      preAllocatedVUs: 15,
      maxVUs: 40,
      startTime: "70s",
      exec: "submitFreeFairness",
    },

    recovery_enterprise: {
      executor: "constant-arrival-rate",
      rate: 10,
      timeUnit: "1s",
      duration: "120s",
      preAllocatedVUs: 20,
      maxVUs: 80,
      startTime: "170s",
      exec: "submitEnterpriseRecovery",
    },
    recovery_free: {
      executor: "constant-arrival-rate",
      rate: 5,
      timeUnit: "1s",
      duration: "120s",
      preAllocatedVUs: 10,
      maxVUs: 30,
      startTime: "170s",
      exec: "submitFreeRecovery",
    },
  },

  thresholds: {
    enterprise_rate_limited: ["count==0"],
    pro_rate_limited: ["count==0"],

    free_burst_rate_limited: ["count>0"],

    job_accepted: ["rate>0.8"],

    job_end_to_end_load_ms: ["p(95)<150"],
    queue_latency_load_ms: ["p(95)<100"],

    job_end_to_end_fairness_ms: ["p(95)<100"],
    queue_latency_fairness_ms: ["p(95)<75"],

    fairness_free_ratio: ["rate>=0.25"],

    recovery_job_accepted: ["rate==1"],
  },
};

const TEST_JPEG = open("./assets/test.jpg", "b");

function submitJob(apiKey, rateLimitedCounter) {
  const res = http.post(
    `${BASE_URL}/jobs`,
    {
      file: http.file(TEST_JPEG, "test.jpg", "image/jpeg"),
    },
    {
      headers: { Authorization: `Bearer ${apiKey}` },
      timeout: "10s", // prevent block from slow response
    }
  );

  if (res.status === 429) {
    rateLimitedCounter.add(1);
    jobAccepted.add(0);
    return null; // expect rate limit
  }

  const accepted = check(res, {
    "job accepted (202)": (r) => r.status === 202,
    "response has job_id": (r) => {
      try {
        return JSON.parse(r.body).job_id !== undefined;
      } catch {
        return false;
      }
    },
  });

  jobAccepted.add(res.status === 202 ? 1 : 0);

  if (!accepted) {
    return null;
  }

  return JSON.parse(res.body).job_id;
}

function waitForCompletion(jobId, apiKey, maxWaitMs = 20000) {
  const start = Date.now();
  while (Date.now() - start < maxWaitMs) {
    const res = http.get(`${BASE_URL}/jobs/${jobId}`, {
      headers: { Authorization: `Bearer ${apiKey}` },
      timeout: "5s",
    });

    if (res.status !== 200) {
      sleep(0.5);
      continue;
    }

    let job;
    try {
      job = JSON.parse(res.body);
    } catch {
      sleep(0.5);
      continue;
    }

    if (job.status === "complete" || job.status === "dead" || job.status === "failed") {
      return job;
    }

    sleep(0.5);
  }
  return null; // timed out
}

export function submitEnterprise() {
  const jobId = submitJob(ENTERPRISE_KEY, enterpriseRateLimited);
  if (!jobId) return;

  const job = waitForCompletion(jobId, ENTERPRISE_KEY);
  if (job && job.status === "complete" && job.created_at && job.updated_at) {
    jobEndToEndLoadMs.add(new Date(job.updated_at) - new Date(job.created_at));
  }
  if (job.created_at && job.processing_started_at) {
    queueLatencyLoadMs.add(new Date(job.processing_started_at) - new Date(job.created_at));
  }
}

export function submitPro() {
  const jobId = submitJob(PRO_KEY, proRateLimited);
  if (!jobId) return;

  const job = waitForCompletion(jobId, PRO_KEY);
  if (job && job.status === "complete" && job.created_at && job.updated_at) {
    jobEndToEndLoadMs.add(new Date(job.updated_at) - new Date(job.created_at));
  }
  if (job.created_at && job.processing_started_at) {
    queueLatencyLoadMs.add(new Date(job.processing_started_at) - new Date(job.created_at));
  }
}

export function submitFree() {
  const jobId = submitJob(FREE_KEY, freeRateLimited);
  if (!jobId) return;
  
  const job = waitForCompletion(jobId, FREE_KEY, 30000);
  if (job && job.status === "complete" && job.created_at && job.updated_at) {
    jobEndToEndLoadMs.add(new Date(job.updated_at) - new Date(job.created_at));
  }
  if (job.created_at && job.processing_started_at) {
    queueLatencyLoadMs.add(new Date(job.processing_started_at) - new Date(job.created_at));
  }
}

export function submitFreeBurst() {
  submitJob(FREE_KEY, freeBurstRateLimited);
}

const recoveryJobAccepted = new Rate("recovery_job_accepted");

export function submitEnterpriseRecovery() {
  const res = http.post(
    `${BASE_URL}/jobs`,
    { file: http.file(TEST_JPEG, "test.jpg", "image/jpeg") },
    { headers: { Authorization: `Bearer ${ENTERPRISE_KEY}` }, timeout: "10s" }
  );

  recoveryJobAccepted.add(res.status === 202 ? 1 : 0);

  check(res, {
    "recovery enterprise accepted (202)": (r) => r.status === 202,
  });
}

export function submitFreeRecovery() {
  const res = http.post(
    `${BASE_URL}/jobs`,
    { file: http.file(TEST_JPEG, "test.jpg", "image/jpeg") },
    { headers: { Authorization: `Bearer ${FREE_KEY}` }, timeout: "10s" }
  );

  recoveryJobAccepted.add(res.status === 202 ? 1 : 0);

  check(res, {
    "recovery free not 5xx": (r) => r.status < 500,
  });
}

export function submitEnterpriseFairness() {
  const jobId = submitJob(ENTERPRISE_KEY, enterpriseRateLimited);
  if (!jobId) return;

  const job = waitForCompletion(jobId, ENTERPRISE_KEY, 30000);
  if (job && job.status === "complete") {
    fairnessFreeRatio.add(0);

    if (job.created_at && job.updated_at) {
      jobEndToEndFairnessMs.add(new Date(job.updated_at) - new Date(job.created_at));
    }
    if (job.created_at && job.processing_started_at) {
      queueLatencyFairnessMs.add(new Date(job.processing_started_at) - new Date(job.created_at));
    }
  } else {
    fairnessFreeRatio.add(0);
  }
}

export function submitFreeFairness() {
  const jobId = submitJob(FREE_KEY, freeRateLimited);
  if (!jobId) return;

  const job = waitForCompletion(jobId, FREE_KEY, 45000);
  if (job && job.status === "complete") {
    fairnessFreeRatio.add(1);

    if (job.created_at && job.updated_at) {
      jobEndToEndFairnessMs.add(new Date(job.updated_at) - new Date(job.created_at));
    }
    if (job.created_at && job.processing_started_at) {
      queueLatencyFairnessMs.add(new Date(job.processing_started_at) - new Date(job.created_at));
    }
  } else {
    fairnessFreeRatio.add(0);
  }
}