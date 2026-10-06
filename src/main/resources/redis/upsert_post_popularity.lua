local member = ARGV[1]
local since = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local lifetime = tonumber(ARGV[4])
local cutoff = now - lifetime
if since > now + 60000 then return -1 end
if since + lifetime <= now then return 0 end
local prior = redis.call('ZSCORE', KEYS[1], member)
if prior then
    local old = tonumber(prior)
    if old + lifetime > now or since <= old then return 0 end
end
redis.call('ZADD', KEYS[1], since, member)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', cutoff)
return 1
