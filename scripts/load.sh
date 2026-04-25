#!/usr/bin/env bash

set -euo pipefail

RUN_TYPE="${1:-baseline}"
RESULTS_FILE="results/${RUN_TYPE}.json"
WORKER_CONTAINER="orchestrator-worker-1"
DB_CONTAINER="orchestrator-db-1"

KILL_OFFSET_SECONDS=200

mkdir -p results

echo "Starting k6 run"
echo "Output results to ${RESULTS_FILE}"
k6 run --out "json=${RESULTS_FILE}" scripts/load.js &
K6_PID=$!

echo "Waiting ${KILL_OFFSET_SECONDS}s before worker SIGKILL"
sleep "${KILL_OFFSET_SECONDS}"

if docker ps --format '{{.Names}}' | grep -q "^${WORKER_CONTAINER}$"; then
  echo "Sending SIGKILL to ${WORKER_CONTAINER}"
  docker kill --signal SIGKILL "${WORKER_CONTAINER}"
else
  echo "Warning: ${WORKER_CONTAINER} not found, skipping kill"
fi

wait "${K6_PID}"
K6_EXIT=$?

echo
echo "k6 finished with exit code ${K6_EXIT}"
echo
echo "Post-run: jobs stuck beyond reaper window"

docker exec "${DB_CONTAINER}" \
  psql -U orchestrator -d orchestrator -t -c \
  "SELECT status, count(*) FROM jobs
   WHERE status = 'processing'
   AND updated_at < now() - interval '75 seconds'
   GROUP BY status;" \
  | xargs echo "  Stuck jobs:"

echo
echo "Results ${RESULTS_FILE}"
echo "Summary: k6 inspect --summary-export=results/${RUN_TYPE}_summary.json ${RESULTS_FILE}"

exit "${K6_EXIT}"