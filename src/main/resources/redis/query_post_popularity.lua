-- Exclusive composite cursor in descending score/reverse-byte-lex order.
-- Binary search works even when the cursor member has expired or been removed.
local upper = tonumber(ARGV[1])
local cutoff = tonumber(ARGV[2])
local anchor = tonumber(ARGV[3])
local anchorId = ARGV[4]
local limit = tonumber(ARGV[5])
local count = redis.call('ZCARD', KEYS[1])
local low, high, first = 0, count - 1, count
while low <= high do
    local mid = math.floor((low + high) / 2)
    local row = redis.call('ZRANGE', KEYS[1], mid, mid, 'REV', 'WITHSCORES')
    local score = tonumber(row[2])
    local after = anchor < 0 or score < anchor or (score == anchor and row[1] < anchorId)
    if score <= upper and after then
        first = mid
        high = mid - 1
    else
        low = mid + 1
    end
end
if first == count then return '[]' end
local rows = redis.call('ZRANGE', KEYS[1], first, first + limit - 1, 'REV', 'WITHSCORES')
local result = {}
for i = 1, #rows, 2 do
    local score = tonumber(rows[i + 1])
    if score <= cutoff then break end
    result[#result + 1] = cjson.encode({postId = rows[i], popularSinceMillis = score})
end
return '[' .. table.concat(result, ',') .. ']'
