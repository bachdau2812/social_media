package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.infrastructure.vector.*;
import com.dauducbach.clone.modules.feed.entity.FeedInteractionProcessing;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import java.util.List;

/** Persistent ST cursor is independent of the profile/LT journal operation marker. */
@Component
public class FeedShortTermRedisState {
    private final ReactiveStringRedisTemplate redis;

    public FeedShortTermRedisState(ReactiveStringRedisTemplate redis) {
        this.redis = redis;
        if (redis.getConnectionFactory() instanceof LettuceConnectionFactory factory
                && factory.getClusterConfiguration() != null)
            throw new IllegalStateException("ST scripts require standalone Redis; migrate key slots first");
    }

    public static String cursorKey(String userId) { return "vector:short_term_cursor:" + userId; }
    public Mono<String> cursor(String userId) { return redis.opsForValue().get(cursorKey(userId)).defaultIfEmpty(""); }

    public record Cursor(String topic, String generation, int partition, long offset, String operationId,
                         String mode, long version) {
        public static Cursor parse(String value) {
            JsonObject json = JsonParser.parseString(value).getAsJsonObject();
            return new Cursor(json.get("topic").getAsString(), json.get("generation").getAsString(),
                    json.get("partition").getAsInt(), json.get("offset").getAsLong(),
                    json.get("operationId").getAsString(), json.get("mode").getAsString(), json.get("version").getAsLong());
        }
        public boolean matches(FeedInteractionProcessing row) {
            return topic.equals(row.getCanonicalTopic()) && generation.equals(row.getCanonicalGeneration())
                    && partition == row.getCanonicalPartition();
        }
    }

    /** expiresAt is absolute SQL lifetime; Redis TIME prevents TTL refresh after delayed delivery. */
    public Mono<Long> commit(VectorLease lease, FeedInteractionProcessing row, String expectedCursor, long expectedVersion) {
        if (!lease.userId().equals(row.getUserId())) return Mono.error(new IllegalArgumentException("Wrong user lease"));
        JsonObject next = new JsonObject();
        next.addProperty("topic", row.getCanonicalTopic());
        next.addProperty("generation", row.getCanonicalGeneration());
        next.addProperty("partition", row.getCanonicalPartition().toString());
        next.addProperty("offset", row.getCanonicalOffset().toString());
        next.addProperty("operationId", row.getOperationId());
        boolean skip = "SKIP_PREPARED".equals(row.getStatus());
        next.addProperty("mode", skip ? "SKIPPED" : "APPLIED");
        String user = row.getUserId();
        return redis.execute(RedisScript.of("""
                if redis.call('GET',KEYS[1]) ~= ARGV[1] then return -1 end
                local prior = redis.call('GET',KEYS[6]) or ''
                local next = cjson.decode(ARGV[4])
                if prior ~= ARGV[2] or (redis.call('GET',KEYS[2]) or '0') ~= ARGV[3] then return -2 end
                if prior ~= '' then
                  local old = cjson.decode(prior)
                  if old.topic ~= next.topic or old.generation ~= next.generation or old.partition ~= next.partition then return -3 end
                  if old.operationId == next.operationId then
                    if old.offset ~= next.offset then return -4 end
                    local current = tonumber(redis.call('GET',KEYS[2]) or '0')
                    if current < tonumber(old.version) and next.mode ~= 'SKIPPED' then return -5 end
                    return current
                  end
                  if #old.offset > #next.offset or (#old.offset == #next.offset and old.offset >= next.offset) then return -4 end
                end
                local version = tonumber(ARGV[3])
                if next.mode ~= 'SKIPPED' then
                  local now = redis.call('TIME')
                  local ttl = tonumber(ARGV[6]) - (tonumber(now[1])*1000 + math.floor(tonumber(now[2])/1000))
                  if ttl > 0 then
                    redis.call('SET',KEYS[4],ARGV[5],'PX',ttl)
                    redis.call('SET',KEYS[5],ARGV[7],'PX',ttl)
                  else
                    redis.call('DEL',KEYS[4],KEYS[5])
                    next.mode = 'EXPIRED'
                  end
                  version = redis.call('INCR',KEYS[2])
                  redis.call('SET',KEYS[3],tostring(version))
                end
                next.version = tostring(version)
                redis.call('SET',KEYS[6],cjson.encode(next))
                return version
                """, Long.class), List.of(VectorCacheKeys.lock(user), VectorCacheKeys.version(user),
                        VectorCacheKeys.dirty(user), VectorCacheKeys.shortTerm(user), VectorCacheKeys.shortTermModel(user), cursorKey(user)),
                List.of(lease.token(), expectedCursor, Long.toString(expectedVersion), next.toString(),
                        skip ? "" : row.getDesiredShortVector(), skip ? "0" : Long.toString(row.getVectorExpiresAt().toEpochMilli()), VectorMath.MODEL))
                .single().flatMap(result -> {
                    if (result == -1) return Mono.error(new UserVectorCoordinator.LeaseLostException());
                    if (result < 0) return Mono.error(new ContinuityException("ST cursor/version conflict (" + result + "); quarantine and inspect durable ledger"));
                    return Mono.just(result);
                });
    }

    public static class ContinuityException extends VectorRepairRequiredException {
        public ContinuityException(String message) { super(message); }
    }
}
