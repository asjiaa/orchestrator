-- Atomically increments a per-tenant-per-second counter
-- Returns the counter value after increment
--
-- KEYS[1] — rl:{tenant_id}:{unix_second}
-- ARGV    — none
--
-- Set TTL to prevent intermittent undercounting at second boundaries

local n = redis.call('INCR', KEYS[1])
if n == 1 then
    redis.call('EXPIRE', KEYS[1], 2)
end
return n