-- Atomically removes a job from inflight lane, decrement counter, push back to tenant lane
--
-- ARGV[1] = serialised job bytes (must match value stored in inflight:jobs)
-- ARGV[2] = tenantID
--
-- Remove first matching occurrence, avoid jobs in terminal state

redis.call('LREM', 'inflight:jobs', 1, ARGV[1])
redis.call('LPUSH', 'queue:tenant:' .. ARGV[2], ARGV[1])
redis.call('DECR', 'inflight:' .. ARGV[2])
return 1