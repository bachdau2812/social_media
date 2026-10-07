package com.dauducbach.clone.architecture;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;

class BackendModuleBoundaryTest {
    private static final Path MAIN_SOURCE = Path.of("src/main/java/com/dauducbach/clone");
    private static final Pattern MODULE_REPOSITORY_IMPORT = Pattern.compile(
            "import\\s+(com\\.dauducbach\\.clone\\.modules\\.([^.]+)\\.repository\\.[\\w.]+);");
    private static final Pattern VOID_KAFKA_LISTENER = Pattern.compile(
            "@KafkaListener\\s*\\([^)]*\\)\\s*public\\s+void\\s+\\w+\\s*\\(");

    @Test
    void frontendCompositionDoesNotImportDomainPersistenceInternals() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE.resolve("modules/frontend"))
                .filter(source -> source.content().contains(".repositoty.")
                        || source.content().contains(".repository.")
                        || source.content().contains(".entity."))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(), () -> "Frontend composition imports domain persistence internals: " + violations);
    }

    @Test
    void frontendScreenCompositionUsesOwningModuleContracts() throws IOException {
        List<String> violations = List.of(
                        MAIN_SOURCE.resolve("modules/frontend/service/HomeScreenService.java"),
                        MAIN_SOURCE.resolve("modules/frontend/service/ProfileScreenService.java"),
                        MAIN_SOURCE.resolve("modules/frontend/dto/HomeScreenResponse.java"),
                        MAIN_SOURCE.resolve("modules/frontend/dto/ProfilePostResponse.java")
                ).stream()
                .filter(Files::exists)
                .map(path -> path.toString().replace('\\', '/') + ": " + read(path))
                .filter(source -> source.contains("modules.feed.service.FeedService")
                        || source.contains("modules.feed.dto.response.FeedResponse")
                        || source.contains("modules.post.service.PostProfileQueryService")
                        || source.contains("modules.post.dto.story.response.StoryTrayResponse")
                        || source.contains("modules.post.dto.response.PostItemResponse")
                        || source.contains("modules.post.dto.response.PostMusicResponse")
                        || source.contains("modules.post.dto.response.PostMediaResponse"))
                .map(source -> source.substring(0, source.indexOf(": ")))
                .toList();

        assertTrue(violations.isEmpty(), () -> "Frontend screen composition depends on internal module services/DTOs: " + violations);
    }

    @Test
    void postNotificationMuteStorageIsOwnedByPostModule() {
        String notificationHandler = read(MAIN_SOURCE.resolve(
                "modules/notification/incoming/post/PushModuleNotificationHandler.java"));
        String mediaInspectionService = read(MAIN_SOURCE.resolve(
                "modules/media/service/MediaInspectionService.java"));

        assertTrue(!notificationHandler.contains("PostNotificationCacheKeys")
                        && !notificationHandler.contains("ReactiveRedisTemplate"),
                "Notification delivery must query post notification preferences through the post contract");
        assertTrue(!mediaInspectionService.contains("modules.media.infrastructure."),
                "Media inspection application service must depend on a scan gateway rather than its HTTP adapter");
    }

    @Test
    void authenticationBootstrapLivesWithAuthCapability() {
        Path globalSecurityConfig = MAIN_SOURCE.resolve("configuration/SecurityConfig.java");
        Path authSecurityConfig = MAIN_SOURCE.resolve("modules/auth/configuration/SecurityConfig.java");
        Path globalCookieConverter = MAIN_SOURCE.resolve("configuration/CookieServerAuthenticationConverter.java");
        Path authCookieConverter = MAIN_SOURCE.resolve(
                "modules/auth/infrastructure/security/CookieServerAuthenticationConverter.java");

        assertTrue(!Files.exists(globalSecurityConfig) && Files.exists(authSecurityConfig)
                        && !Files.exists(globalCookieConverter) && Files.exists(authCookieConverter),
                "Authentication bootstrap and cookie conversion belong to the auth module");
    }

    @Test
    void sharedPayloadAndRedisCodecsHaveExplicitPackages() {
        assertMoved("utils/GsonUtils.java", "commons/serialization/GsonUtils.java");
        assertMoved("utils/JsonPayloadReader.java", "commons/serialization/JsonPayloadReader.java");
        assertMoved("utils/RedisJsonCodec.java", "infrastructure/redis/RedisJsonCodec.java");
    }

    private void assertMoved(String oldPath, String newPath) {
        assertTrue(!Files.exists(MAIN_SOURCE.resolve(oldPath)) && Files.exists(MAIN_SOURCE.resolve(newPath)),
                () -> oldPath + " must be replaced by " + newPath);
    }

    @Test
    void repositoryPackagesUseTheCorrectSpelling() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE.resolve("modules"))
                .filter(source -> source.relativePath().contains("/repositoty/"))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(), () -> "Repository packages contain a spelling error: " + violations);
    }

    @Test
    void authenticationControllerDoesNotLogResolvedTokenRequests() {
        String content = read(MAIN_SOURCE.resolve("modules/auth/controller/AuthenticationController.java"));
        assertTrue(!content.contains("Resolved refresh token request"),
                "AuthenticationController must not log requests containing refresh tokens");
    }

    @Test
    void authenticationUseCasesDoNotDependOnInfrastructureImplementations() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE.resolve("modules/auth"))
                .filter(source -> source.relativePath().startsWith("modules/auth/login/")
                        || source.relativePath().startsWith("modules/auth/sessions/")
                        || source.relativePath().startsWith("modules/auth/registration/")
                        || source.relativePath().startsWith("modules/auth/recovery/"))
                .filter(source -> source.content().lines().anyMatch(line -> line.startsWith("import ")
                        && (line.contains("modules.auth.repository.")
                        || line.contains("modules.auth.entity.")
                        || line.contains("modules.auth.infrastructure.")
                        || line.contains("org.springframework.data.")
                        || line.contains("org.springframework.security.crypto."))))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(), () -> "Auth use cases import persistence/security adapters: " + violations);
    }

    @Test
    void authenticationLogsDoNotExposeCredentialsChallengesOrProviderProfileData() throws IOException {
        List<String> authSources = javaSources(MAIN_SOURCE.resolve("modules/auth"))
                .map(SourceFile::content)
                .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new));
        authSources.add(read(MAIN_SOURCE.resolve(
                "modules/auth/infrastructure/security/CookieServerAuthenticationConverter.java")));
        String source = String.join("\n", authSources);

        List<String> forbiddenLogValues = List.of(
                "request.toString()",
                "request={}",
                "expectedCode={}",
                "actualCode={}",
                "getCodeFromRedis={}",
                "codeExpired=false({})",
                "getAttributes())",
                "email={}",
                "providerId={}",
                "avatarUrl={}",
                "redirectUrl: {}",
                "deviceInfo={}");
        List<String> violations = forbiddenLogValues.stream()
                .filter(source::contains)
                .toList();

        assertTrue(violations.isEmpty(), () -> "Authentication sources log sensitive values: " + violations);
    }

    @Test
    void modulesOutsideMediaUseMediaCompatibilityBoundary() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE.resolve("modules"))
                .filter(source -> !source.relativePath().startsWith("modules/media/"))
                .filter(source -> source.content().contains(
                        "import com.dauducbach.clone.modules.media.infrastructure."))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(), () -> "Modules import a vendor-specific Media adapter: " + violations);
    }

    @Test
    void codeOutsideMediaDoesNotUseCloudinarySdkOrUtility() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE)
                .filter(source -> !source.relativePath().startsWith("modules/media/"))
                .filter(source -> source.content().contains("import com.cloudinary.")
                        || source.content().contains("import com.dauducbach.clone.modules.media.infrastructure."))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(), () -> "Modules bypass Media Cloudinary ownership: " + violations);
    }

    @Test
    void modulesOutsideMediaDoNotImportMediaRepository() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE.resolve("modules"))
                .filter(source -> !source.relativePath().startsWith("modules/media/"))
                .filter(source -> source.content().contains(
                        "import com.dauducbach.clone.modules.media.repository.MediaRepository;"))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(), () -> "Modules bypass Media registry ownership: " + violations);
    }

    @Test
    void modulesOutsideMediaDoNotWriteMediaTableDirectly() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE.resolve("modules"))
                .filter(source -> !source.relativePath().startsWith("modules/media/"))
                .filter(source -> source.content().contains("insert(Media.class)"))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(), () -> "Modules write Media persistence directly: " + violations);
    }

    @Test
    void kafkaListenersExposeProcessingCompletionToTheContainer() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE.resolve("modules"))
                .filter(source -> VOID_KAFKA_LISTENER.matcher(source.content()).find())
                .filter(source -> !usesBoundedBlockingKafkaAdapter(source))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(), () -> "Kafka listeners acknowledge before reactive processing completes: " + violations);
    }

    @Test
    void popularityProjectionListenerWaitsOnItsDedicatedConsumerThread() throws IOException {
        SourceFile listener = javaSources(MAIN_SOURCE.resolve("modules"))
                .filter(source -> source.relativePath().equals("modules/post/listener/PostPopularityProjectionListener.java"))
                .findFirst()
                .orElseThrow();

        assertTrue(usesBoundedBlockingKafkaAdapter(listener),
                "Popularity projection may use bounded blocking only on its dedicated Kafka consumer thread");
    }

    private boolean usesBoundedBlockingKafkaAdapter(SourceFile source) {
        return source.relativePath().equals("modules/post/listener/PostPopularityProjectionListener.java")
                && source.content().contains("projection.apply(update).block(PROJECTION_TIMEOUT)")
                && source.content().contains("dedicated Kafka consumer thread");
    }

    @Test
    void modulesDoNotImportAnotherModulesRepository() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE.resolve("modules"))
                .flatMap(source -> {
                    String[] pathParts = source.relativePath().split("/");
                    String sourceModule = pathParts.length > 1 ? pathParts[1] : "";
                    java.util.regex.Matcher matcher = MODULE_REPOSITORY_IMPORT.matcher(source.content());
                    java.util.List<String> matches = new java.util.ArrayList<>();
                    while (matcher.find()) {
                        if (!sourceModule.equals(matcher.group(2))) {
                            matches.add(source.relativePath() + " -> " + matcher.group(1));
                        }
                    }
                    return matches.stream();
                })
                .toList();

        assertTrue(violations.isEmpty(), () -> "Cross-module repository imports: " + violations);
    }

    @Test
    void moduleDependenciesDoNotContainCycles() throws IOException {
        Map<String, Set<String>> dependencies = new HashMap<>();
        Pattern moduleImport = Pattern.compile(
                "^import\\s+com\\.dauducbach\\.clone\\.modules\\.([^.]+)\\.");
        javaSources(MAIN_SOURCE.resolve("modules")).forEach(source -> {
            String[] pathParts = source.relativePath().split("/");
            if (pathParts.length < 2) return;
            String sourceModule = pathParts[1];
            Set<String> moduleDependencies = dependencies.computeIfAbsent(sourceModule, ignored -> new HashSet<>());
            source.content().lines()
                    .filter(line -> line.startsWith("import "))
                    .forEach(line -> {
                        java.util.regex.Matcher matcher = moduleImport.matcher(line);
                        if (matcher.find() && !sourceModule.equals(matcher.group(1))) {
                            moduleDependencies.add(matcher.group(1));
                        }
                    });
        });

        Set<String> visited = new HashSet<>();
        Set<String> inProgress = new HashSet<>();
        Deque<String> path = new ArrayDeque<>();
        List<String> cycles = new ArrayList<>();
        for (String module : dependencies.keySet()) {
            collectDependencyCycles(module, dependencies, visited, inProgress, path, cycles);
        }

        assertTrue(cycles.isEmpty(), () -> "Cyclic module dependencies: " + cycles);
    }

    private void collectDependencyCycles(
            String module,
            Map<String, Set<String>> dependencies,
            Set<String> visited,
            Set<String> inProgress,
            Deque<String> path,
            List<String> cycles
    ) {
        if (inProgress.contains(module)) {
            List<String> cycle = new ArrayList<>();
            boolean inCycle = false;
            for (String pathModule : path) {
                if (pathModule.equals(module)) inCycle = true;
                if (inCycle) cycle.add(pathModule);
            }
            cycle.add(module);
            cycles.add(String.join(" -> ", cycle));
            return;
        }
        if (!visited.add(module)) return;

        inProgress.add(module);
        path.addLast(module);
        for (String dependency : dependencies.getOrDefault(module, Set.of())) {
            collectDependencyCycles(dependency, dependencies, visited, inProgress, path, cycles);
        }
        path.removeLast();
        inProgress.remove(module);
    }

    @Test
    void customSqlIsDeclaredByPersistenceAdapters() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE.resolve("modules"))
                .filter(source -> source.content().contains("@Query")
                        || source.content().contains("org.springframework.r2dbc.core.DatabaseClient")
                        || source.content().contains(".sql("))
                .filter(source -> !source.relativePath().contains("/repository/")
                        && !source.relativePath().contains("/infrastructure/persistence/"))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(),
                () -> "Custom SQL must live in the owning module's persistence adapter: " + violations);
    }

    @Test
    void scheduledBusinessJobsReturnReactiveWorkInsteadOfSubscribingInternally() {
        Map<String, String> scheduledJobs = Map.of(
                "modules/personalization/longterm/LongTermPreferenceRefreshService.java",
                "public Mono<Void> updateYesterdayLongTermVectors()",
                "modules/personalization/discovery/UserDiscoveryUseCase.java",
                "public Mono<Void> refreshDailySuggestions()",
                "modules/post/service/post/PostPopularityProjectionService.java",
                "public Mono<Void> pruneExpired()",
                "modules/post/service/post/PostInteractionService.java",
                "public Mono<Void> cleanupReceipts()");
        List<String> violations = scheduledJobs.entrySet().stream()
                .filter(entry -> {
                    String source = read(MAIN_SOURCE.resolve(entry.getKey()));
                    return !source.contains("@Scheduled") || !source.contains(entry.getValue())
                            || source.contains(".subscribe(");
                })
                .map(Map.Entry::getKey)
                .toList();

        assertTrue(violations.isEmpty(),
                () -> "Scheduled business work must be returned to Spring's reactive scheduler: " + violations);
    }

    @Test
    void modulesOutsideAuditUseItsPublicContractsInsteadOfEntitiesOrServices() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE.resolve("modules"))
                .filter(source -> !source.relativePath().startsWith("modules/audit/"))
                .filter(source -> source.content().lines().anyMatch(line -> line.startsWith("import ")
                        && line.contains("com.dauducbach.clone.modules.audit.")
                        && (line.contains(".entity.") || line.contains(".service.")
                        || line.contains(".repository.") || line.contains(".repositoty."))))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(), () -> "Modules bypass Audit public contracts: " + violations);
    }

    @Test
    void modulesOutsideAuditDoNotImportAuditDtos() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE.resolve("modules"))
                .filter(source -> !source.relativePath().startsWith("modules/audit/"))
                .filter(source -> source.content().contains("import com.dauducbach.clone.modules.audit.dto."))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(), () -> "Modules must use Audit public contracts, not Audit DTOs: " + violations);
    }

    @Test
    void userDoesNotDependOnPostSseImplementation() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE.resolve("modules/user"))
                .filter(source -> source.content().contains("modules.post.service.post.PostSseService"))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(), () -> "User depends on Post SSE implementation: " + violations);
    }

    @Test
    void auditDoesNotDependOnFeedContracts() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE.resolve("modules/audit"))
                .filter(source -> source.content().contains("modules.feed."))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(), () -> "Audit depends on Feed implementation or DTOs: " + violations);
    }

    @Test
    void publicApiPackagesDoNotExposePersistenceOrInfrastructureTypes() throws IOException {
        Pattern forbiddenPublicType = Pattern.compile(
                "import\\s+com\\.dauducbach\\.clone\\.modules\\.[^.]+\\.(?:entity|repository|infrastructure|service|controller)\\.");
        List<String> violations = javaSources(MAIN_SOURCE.resolve("modules"))
                .filter(source -> source.relativePath().contains("/publicapi/"))
                .filter(source -> forbiddenPublicType.matcher(source.content()).find())
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(), () -> "Public module APIs expose implementation types: " + violations);
    }

    @Test
    void modulesOutsideMediaDependOnMediaPublicContracts() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE.resolve("modules"))
                .filter(source -> !source.relativePath().startsWith("modules/media/"))
                .filter(source -> source.content().lines().anyMatch(line -> line.startsWith("import ")
                        && line.matches(".*modules\\.media\\.(?:entity|repository|service|configuration|infrastructure)\\..*")))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(), () -> "Modules bypass Media public contracts: " + violations);
    }

    @Test
    void mediaMusicUseCasesDoNotDependOnInfrastructureImplementations() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE.resolve("modules/media/music"))
                .filter(source -> source.content().lines().anyMatch(line -> line.startsWith("import ")
                        && line.matches(".*modules\\.media\\.(?:infrastructure|repository|entity|service)\\..*")))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(), () -> "Media music use cases import implementation details: " + violations);
    }

    @Test
    void controllersDoNotImportRepositories() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE.resolve("modules"))
                .filter(source -> source.relativePath().contains("/controller/"))
                .filter(source -> source.content().contains(".repository.")
                        || source.content().contains(".repositoty."))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(), () -> "Controllers import repositories: " + violations);
    }

    @Test
    void chatDoesNotImportUserPersistenceInternals() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE.resolve("modules/chat"))
                .filter(source -> source.content().contains("modules.user.repository")
                        || source.content().contains("modules.user.entity"))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(), () -> "Chat imports User/Story persistence internals: " + violations);
    }

    @Test
    void modulesOutsideUserUsePublicIdentityContracts() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE.resolve("modules"))
                .filter(source -> !source.relativePath().startsWith("modules/user/"))
                .filter(source -> source.content().lines().anyMatch(line -> line.startsWith("import ")
                        && (line.contains("modules.user.service.UserIdentityQueryService")
                        || line.contains("modules.user.service.UserDetailsService")
                        || line.contains("modules.user.service.MediaForProfile")
                        || line.contains("modules.user.service.UserProfileCompositionQueryService")
                        || line.contains("modules.user.service.UserFollowerService")
                        || line.contains("modules.user.entity.UserDetails")
                        || line.contains("modules.user.repository.UserDetailsRepository"))))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(), () -> "Modules bypass User identity contracts: " + violations);
    }

    @Test
    void personalizationDoesNotDependOnFeedImplementation() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE.resolve("modules/personalization"))
                .filter(source -> source.content().lines().anyMatch(line -> line.startsWith("import ")
                        && line.contains("com.dauducbach.clone.modules.feed.")))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(), () -> "Personalization imports Feed implementation or DTOs: " + violations);
    }

    @Test
    void notificationDoesNotImportOtherModuleRepositoriesOrEntities() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE.resolve("modules/notification"))
                .filter(source -> source.content().lines()
                        .anyMatch(line -> line.startsWith("import com.dauducbach.clone.modules.")
                                && !line.startsWith("import com.dauducbach.clone.modules.notification.")
                                && (line.contains(".repository.")
                                || line.contains(".repositoty.")
                                || line.contains(".entity."))))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(), () -> "Notification imports another module's persistence internals: " + violations);
    }
    @Test
    void postDeliveryQueriesExcludeArchivedPosts() {
        String content = read(MAIN_SOURCE.resolve(
                "modules/post/repository/PostDetailsRepository.java"));
        assertTrue(content.contains("findApprovedFeedEligibleById")
                        && content.contains("NOT EXISTS")
                        && content.contains("user_archive_items"),
                "Post delivery/profile queries must exclude archived posts at the source query");
    }

    @Test
    void postServiceDoesNotStartDetachedReactiveWork() throws IOException {
        String content = read(MAIN_SOURCE.resolve("modules/post/service/post/PostService.java"));
        assertTrue(!content.contains(".subscribe(") && !content.contains(".subscribe();"),
                "PostService must compose cache and event work into the returned reactive chain");
    }

    @Test
    void userDetailsServiceUsesProfileCachePortWithoutDetachedRedisWork() {
        String content = read(MAIN_SOURCE.resolve("modules/user/service/UserDetailsService.java"));
        assertTrue(!content.contains(".subscribe(")
                        && !content.contains("ReactiveRedisTemplate")
                        && !content.contains("RedisJsonCodec"),
                "UserDetailsService must compose cache work through the profile cache boundary");
    }

    @Test
    void profileRecordServicesUseCachePortWithoutDetachedRedisWork() throws IOException {
        List<String> profileRecords = List.of(
                "modules/user/service/UserJobService.java",
                "modules/user/service/UserUniversityService.java",
                "modules/user/service/UserHighSchoolService.java",
                "modules/user/service/UserPhoneService.java",
                "modules/user/service/UserSocialMediaService.java");
        List<String> violations = profileRecords.stream()
                .filter(path -> {
                    String content = read(MAIN_SOURCE.resolve(path));
                    return content.contains("ReactiveRedisTemplate")
                            || content.contains("RedisJsonCodec")
                            || content.contains(".subscribe(");
                })
                .toList();

        assertTrue(violations.isEmpty(),
                () -> "Profile record services must compose cache work through ProfileDataCache: " + violations);
    }

    @Test
    void userProfileAndFollowUseCasesDoNotImportPersistenceOrBrokerClients() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE.resolve("modules/user"))
                .filter(source -> source.relativePath().startsWith("modules/user/profile/application/")
                        || source.relativePath().startsWith("modules/user/relationship/application/"))
                .filter(source -> source.content().lines().anyMatch(line -> line.startsWith("import ")
                        && (line.contains("modules.user.repository.")
                        || line.contains("modules.user.infrastructure.")
                        || line.contains("org.springframework.data.")
                        || line.contains("org.springframework.kafka.")
                        || line.contains("org.apache.kafka.")
                        || line.contains("reactor.kafka."))))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(), () -> "User use cases import persistence or broker clients: " + violations);
    }

    @Test
    void userSearchApplicationDoesNotImportPersistenceOrFrameworkAdapters() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE.resolve("modules/user/search/application"))
                .filter(source -> source.content().lines().anyMatch(line -> line.startsWith("import ")
                        && (line.contains("modules.user.repository.")
                        || line.contains("modules.user.infrastructure.")
                        || line.contains("com.dauducbach.clone.infrastructure.")
                        || line.contains("org.springframework.data.")
                        || line.contains("org.springframework.stereotype."))))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(), () -> "User search application imports infrastructure: " + violations);
    }

    @Test
    void searchSuggestionServiceUsesCachePortInsteadOfRedisClient() {
        String content = read(MAIN_SOURCE.resolve("modules/user/service/SearchSuggestionService.java"));
        assertTrue(content.contains("SearchSuggestionCache")
                        && !content.contains("ReactiveRedisTemplate")
                        && !content.contains("org.springframework.data.redis"),
                "Search suggestion behavior must depend on its cache contract, not Redis implementation");
    }

    @Test
    void commentServiceDelegatesCountCacheCoordinationToItsCapabilityAdapter() {
        String content = read(MAIN_SOURCE.resolve("modules/post/comments/application/CommentWriteService.java"));
        assertTrue(content.contains("CommentCountCache")
                        && !content.contains("post_comment_count_lock:")
                        && !content.contains("COUNT_LOCK_RETRY"),
                "CommentWriteService must not own Redis comment-count locking and cache-key mechanics");
    }

    @Test
    void postPublicContractsDoNotExposePersistenceOrFrameworkTypes() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE.resolve("modules/post/publicapi"))
                .filter(source -> source.content().lines().anyMatch(line -> line.startsWith("import ")
                        && (line.contains("modules.post.entity.")
                        || line.contains("modules.post.repository.")
                        || line.contains("modules.post.service.")
                        || line.contains("modules.post.infrastructure.")
                        || line.contains("org.springframework."))))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(), () -> "Post public contracts expose internal implementation: " + violations);
    }

    @Test
    void notificationPostReadsUsePublicQueryContracts() {
        String content = read(MAIN_SOURCE.resolve("modules/notification/incoming/post/PushModuleNotificationHandler.java"));
        assertTrue(content.contains("modules.post.publicapi.PostQuery")
                        && content.contains("modules.post.publicapi.PostInteractionQuery")
                        && content.contains("modules.post.publicapi.CommentQuery")
                        && !content.contains("modules.post.service."),
                "Notifications must depend on post-owned query contracts, not service implementations");
    }

    @Test
    void notificationIncomingAdaptersUseChatPublicContracts() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE.resolve("modules/notification/incoming/chat"))
                .filter(source -> source.content().lines().anyMatch(line -> line.startsWith("import ")
                        && (line.contains("modules.chat.service.")
                        || line.contains("modules.chat.repository.")
                        || line.contains("modules.chat.entity."))))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(),
                () -> "Notification chat adapters import chat implementations: " + violations);
    }

    @Test
    void chatMessageHistoryUsesBatchedOwnerSnapshotsAndKeepsInboxJoinBudgetExplicit() {
        String reads = read(MAIN_SOURCE.resolve("modules/chat/repository/ChatReadRepository.java"));
        String after = reads.substring(reads.indexOf("public Flux<ChatMessage> findAfterSequence"),
                reads.indexOf("public Flux<ChatMessage> findBeforeSequence"));
        String before = reads.substring(reads.indexOf("public Flux<ChatMessage> findBeforeSequence"),
                reads.indexOf("private ChatMessage mapMessage"));
        String messageQueries = after + before;
        assertTrue(!messageQueries.contains("user_details") && !messageQueries.contains("FROM media"),
                "Chat history must not hide user/media ownership joins inside message SQL");
        assertTrue(reads.contains("Read-only query-budget exception: direct inbox title/avatar")
                        && reads.contains("single bounded SQL page query"),
                "The direct-peer inbox join is an explicit one-query read-model exception");

        String query = read(MAIN_SOURCE.resolve("modules/chat/service/ChatMessageQueryService.java"));
        assertTrue(query.contains("UserIdentityQuery") && query.contains("MediaCatalog")
                        && query.contains("findIdentities(userIds)") && query.contains("findCurrentAvatars(userIds)"),
                "Message display snapshots must use one batch call per owner API for each page");
    }

    @Test
    void notificationDeliveryUseCaseUsesModulePortsInsteadOfInfrastructure() throws IOException {
        String content = read(MAIN_SOURCE.resolve("modules/notification/delivery/DeliverNotificationUseCase.java"));
        assertTrue(content.contains("NotificationPersistence")
                        && content.contains("NotificationPushTokenQuery")
                        && content.contains("NotificationRealtimePublisher")
                        && content.contains("NotificationPushGateway")
                        && !content.contains("R2dbcEntityTemplate")
                        && !content.contains("TransactionalOperator")
                        && !content.contains("modules.notification.infrastructure."),
                "Notification delivery policy must call ports; R2DBC, transactions, push provider, and realtime stay adapters");
    }

    @Test
    void postServiceDelegatesRedisAndPublicationOperationsToPorts() {
        String content = read(MAIN_SOURCE.resolve("modules/post/service/post/PostService.java"));
        assertTrue(content.contains("PostDetailsCache") && content.contains("PostNotificationMuteStore")
                        && content.contains("PostPublicationMessaging") && !content.contains("ReactiveRedisTemplate")
                        && !content.contains("RedisJsonCodec") && !content.contains("KafkaSender")
                        && !content.contains("ProducerRecord") && !content.contains("PostSseService"),
                "Post service must use post-owned cache and publication ports, not infrastructure transports");
    }

    @Test
    void feedUsesPersonalizationPublicContractsInsteadOfItsInfrastructure() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE.resolve("modules/feed"))
                .filter(source -> source.content().lines().anyMatch(line -> line.startsWith("import ")
                        && (line.contains("modules.personalization.infrastructure.")
                        || line.contains("modules.personalization.snapshots."))))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(),
                () -> "Feed imports personalization implementations: " + violations);
    }

    @Test
    void vectorPoliciesAreNotOwnedByGenericInfrastructure() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE.resolve("modules/personalization"))
                .filter(source -> !source.relativePath().startsWith("modules/personalization/infrastructure/"))
                .filter(source -> source.content().lines().anyMatch(line -> line.startsWith("import ")
                        && line.contains("com.dauducbach.clone.infrastructure.vector.")))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(),
                () -> "Personalization imports generic vector infrastructure: " + violations);
    }

    @Test
    void featureModulesUseEmbeddingAndSemanticSearchPublicContracts() throws IOException {
        List<String> embeddingViolations = javaSources(MAIN_SOURCE.resolve("modules"))
                .filter(source -> !source.relativePath().startsWith("modules/embedding/"))
                .filter(source -> source.content().lines().anyMatch(line -> line.startsWith("import ")
                        && (line.contains("modules.embedding.infrastructure.")
                        || line.contains("modules.embedding.GeminiEmbeddingProvider")
                        || line.contains("utils.GetVectorEmbedding"))))
                .map(SourceFile::relativePath)
                .toList();
        List<String> searchViolations = javaSources(MAIN_SOURCE.resolve("modules"))
                .filter(source -> !source.relativePath().startsWith("modules/semanticsearch/"))
                .filter(source -> source.content().lines().anyMatch(line -> line.startsWith("import ")
                        && line.contains("modules.semanticsearch.infrastructure.")))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(embeddingViolations.isEmpty(),
                () -> "Feature modules import Gemini implementation: " + embeddingViolations);
        assertTrue(searchViolations.isEmpty(),
                () -> "Feature modules import semantic search implementation: " + searchViolations);
    }

    private java.util.stream.Stream<SourceFile> javaSources(Path root) throws IOException {
        return Files.walk(root)
                .filter(Files::isRegularFile)
                .filter(path -> path.toString().endsWith(".java"))
                .map(path -> new SourceFile(
                        MAIN_SOURCE.relativize(path).toString().replace('\\', '/'),
                        read(path)));
    }

    private String read(Path path) {
        try {
            return Files.readString(path);
        } catch (IOException error) {
            throw new IllegalStateException("Cannot read " + path, error);
        }
    }

    private record SourceFile(String relativePath, String content) {
    }
}
