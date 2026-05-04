package com.dauducbach.clone.configuration;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j

public class StartupConnectionChecker {
    private final DatabaseClient databaseClient;
    private final ReactiveRedisConnectionFactory redisConnectionFactory;

    @EventListener(ApplicationReadyEvent.class)
    public void checkConnectionsOnStartup() {
        // 1. Kiểm tra R2DBC (Database)
        Mono<Void> checkR2dbc = databaseClient.sql("SELECT 1")
                .fetch()
                .first()
                .doOnSuccess(res -> log.info("🟢 R2DBC: Kết nối thành công!"))
                .doOnError(err -> log.error("🔴 R2DBC: Lỗi kết nối - {}", err.getMessage()))
                .then()
                .onErrorResume(e -> Mono.error(e.getCause()));

        // 2. Kiểm tra Redis
        Mono<Void> checkRedis = redisConnectionFactory.getReactiveConnection().ping()
                .doOnSuccess(res -> log.info("🟢 Redis: Kết nối thành công! (Phản hồi: {})", res))
                .doOnError(err -> log.error("🔴 Redis: Lỗi kết nối - {}", err.getMessage()))
                .then()
                .onErrorResume(e -> Mono.empty());

        // 3. Kiem tra Kafka
        Mono<Void> checkKafka = Mono.defer(() -> {
            try (AdminClient admin = AdminClient.create(
                    Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"))) {

                admin.describeCluster().nodes().get();
                log.info("🟢 Kafka connected OK");
            } catch (Exception e) {
                log.info("🔴 Kafka connection failed - {}", e.getMessage());
            }

            return Mono.empty();
        });

        // Gộp cả 2 tiến trình chạy song song và BẮT BUỘC phải gọi .subscribe() để thực thi
        Mono.when(checkR2dbc, checkRedis, checkKafka)
                .subscribe(
                        null, 
                        err -> log.error("🔴 Lỗi không xác định khi kiểm tra kết nối: ", err)
                );
    }
}