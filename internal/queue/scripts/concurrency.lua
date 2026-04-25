-- Atomically increments per-tenant inflight counter and validate on concurrency limit
--
-- KEYS[1] — inflight:{tenant_id}
-- ARGV[1] — max_concurrent (integer cap)
--
-- Returns 1 if the increment was accepted for headroom existed
-- Returns 0 if the cap was already reached for increment rolled back

-- Check and increment as single operation to handle race condition on concurrent calls

local n = redis.call('INCR', KEYS[1])
if n > tonumber(ARGV[1]) then
    redis.call('DECR', KEYS[1])
    return 0
end
return 1