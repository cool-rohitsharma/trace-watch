-- KEYS[1] = idempotency key (e.g. idem:tenantA:uuid-123)
-- KEYS[2] = rate limit window key (e.g. rl:tenantA:1732345600)
-- ARGV[1] = idempotency TTL in seconds
-- ARGV[2] = max requests per second for tenant

-- 1. Reject duplicate retries immediately without consuming quota
if redis.call('EXISTS', KEYS[1]) == 1 then
    return 'DUPLICATE'
end

-- 2. Increment per-second tenant counter and enforce quota
local current = redis.call('INCR', KEYS[2])
if current == 1 then
    redis.call('EXPIRE', KEYS[2], 2)
end

if current > tonumber(ARGV[2]) then
    return 'RATE_LIMITED'
end

-- 3. Mark idempotency key as accepted
redis.call('SET', KEYS[1], '1', 'EX', tonumber(ARGV[1]))
return 'ALLOWED'