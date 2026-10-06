package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.audit.service.UserAuditService;
import com.dauducbach.clone.modules.user.dto.request.*;
import com.dauducbach.clone.modules.user.entity.*;
import com.dauducbach.clone.modules.user.repositoty.*;
import com.dauducbach.clone.utils.RedisUtil;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import reactor.core.publisher.Mono;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

/** Stateful SQL/cache boundary: only subscribed SQL/cache operations change observable state. */
class ProfileCacheFixture {
    @SuppressWarnings("unchecked") final ReactiveRedisTemplate<String, String> redis = mock(ReactiveRedisTemplate.class);
    @SuppressWarnings("unchecked") final ReactiveValueOperations<String, String> values = mock(ReactiveValueOperations.class);
    final Map<String, String> cache = new ConcurrentHashMap<>();
    final Map<String, Duration> ttls = new ConcurrentHashMap<>();
    final R2dbcEntityTemplate template = mock(R2dbcEntityTemplate.class, RETURNS_DEEP_STUBS);
    final UserAuditService audit = mock(UserAuditService.class);
    final UserProfileVectorEventPublisher publisher = mock(UserProfileVectorEventPublisher.class);
    final IllegalStateException publishFailure = new IllegalStateException("broker rejected refresh");
    Object sqlRow;
    int sqlMutations;

    ProfileCacheFixture() {
        when(redis.opsForValue()).thenReturn(values);
        when(values.get(anyString())).thenAnswer(call -> Mono.defer(() -> Mono.justOrEmpty(cache.get(call.getArgument(0)))));
        when(values.set(anyString(), anyString(), any(Duration.class))).thenAnswer(call -> Mono.fromSupplier(() -> {
            cache.put(call.getArgument(0), call.getArgument(1)); ttls.put(call.getArgument(0), call.getArgument(2)); return true;
        }));
        when(values.delete(anyString())).thenAnswer(call -> Mono.fromSupplier(() -> cache.remove(call.getArgument(0)) != null));
        when(audit.save(any())).thenReturn(Mono.empty());
        when(publisher.publishRefreshEvent(anyString(), anyString(), anyString(), anyString())).thenReturn(Mono.error(publishFailure));
        when(publisher.publishRefreshEventForCreatedUser(anyString(), anyString(), anyString(), anyString(), any())).thenReturn(Mono.error(publishFailure));
    }
    <T> void insertion(Class<T> type) {
        when(template.insert(type).using(any(type))).thenAnswer(call -> Mono.fromSupplier(() -> {
            sqlRow = call.getArgument(0); sqlMutations++; return sqlRow;
        }));
    }
    void seed(String key, Object row) { cache.put(key, RedisUtil.serialize(row)); }
    enum Component {
        JOB("user_job"), HIGH_SCHOOL("user_high_school"), UNIVERSITY("user_university");
        final String prefix;
        Component(String prefix) { this.prefix = prefix; }
    }
    Mono<?> componentMutation(Component component, String operation) {
        switch (component) {
            case JOB -> {
                var repository = mock(UserJobRepository.class);
                var row = UserJob.builder().id("component").userId("u").companyName("Before").isPublic(true).build();
                seed(component.prefix + ":component", row); seed(component.prefix + "_list:u", java.util.List.of(row));
                sqlRow = row;
                when(repository.findById("component")).thenAnswer(call -> Mono.justOrEmpty((UserJob) sqlRow));
                when(repository.save(any())).thenAnswer(call -> Mono.fromSupplier(() -> { sqlRow = call.getArgument(0); sqlMutations++; return (UserJob) sqlRow; }));
                when(repository.deleteById("component")).thenReturn(Mono.fromRunnable(() -> { sqlRow = null; sqlMutations++; }));
                var service = new UserJobService(repository, template, redis, audit, publisher);
                if (operation.equals("CREATE")) { insertion(UserJob.class); return service.createUserJob(UserJobRequest.builder().userId("u").companyName("After").build()); }
                if (operation.equals("UPDATE")) return service.updateUserJob(UserJobUpdateRequest.builder().id("component").companyName("After").build());
                return service.deleteUserJob("component");
            }
            case HIGH_SCHOOL -> {
                var repository = mock(UserHighSchoolRepository.class);
                var row = UserHighSchool.builder().id("component").userId("u").schoolName("Before").isPublic(true).build();
                seed(component.prefix + ":component", row); seed(component.prefix + "_list:u", java.util.List.of(row));
                sqlRow = row;
                when(repository.findById("component")).thenAnswer(call -> Mono.justOrEmpty((UserHighSchool) sqlRow));
                when(repository.save(any())).thenAnswer(call -> Mono.fromSupplier(() -> { sqlRow = call.getArgument(0); sqlMutations++; return (UserHighSchool) sqlRow; }));
                when(repository.deleteById("component")).thenReturn(Mono.fromRunnable(() -> { sqlRow = null; sqlMutations++; }));
                var service = new UserHighSchoolService(repository, template, redis, audit, publisher);
                if (operation.equals("CREATE")) { insertion(UserHighSchool.class); return service.createUserHighSchool(UserHighSchoolRequest.builder().userId("u").schoolName("After").build()); }
                if (operation.equals("UPDATE")) return service.updateUserHighSchool(UserHighSchoolRequest.builder().id("component").schoolName("After").build());
                return service.deleteUserHighSchool("component");
            }
            case UNIVERSITY -> {
                var repository = mock(UserUniversityRepository.class);
                var row = UserUniversity.builder().id("component").userId("u").schoolName("Before").isPublic(true).build();
                seed(component.prefix + ":component", row); seed(component.prefix + "_list:u", java.util.List.of(row));
                sqlRow = row;
                when(repository.findById("component")).thenAnswer(call -> Mono.justOrEmpty((UserUniversity) sqlRow));
                when(repository.save(any())).thenAnswer(call -> Mono.fromSupplier(() -> { sqlRow = call.getArgument(0); sqlMutations++; return (UserUniversity) sqlRow; }));
                when(repository.deleteById("component")).thenReturn(Mono.fromRunnable(() -> { sqlRow = null; sqlMutations++; }));
                var service = new UserUniversityService(repository, template, redis, audit, publisher);
                if (operation.equals("CREATE")) { insertion(UserUniversity.class); return service.createUserUniversity(UserUniversityRequest.builder().userId("u").schoolName("After").build()); }
                if (operation.equals("UPDATE")) return service.updateUserUniversity(UserUniversityRequest.builder().id("component").schoolName("After").build());
                return service.deleteUserUniversity("component");
            }
            default -> throw new IllegalStateException();
        }
    }
    String componentId() {
        if (sqlRow instanceof UserJob row) return row.getId();
        if (sqlRow instanceof UserHighSchool row) return row.getId();
        return ((UserUniversity) sqlRow).getId();
    }
}
