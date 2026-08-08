package com.dauducbach.clone.architecture;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;

class BackendModuleBoundaryTest {
    private static final Path MAIN_SOURCE = Path.of("src/main/java/com/dauducbach/clone");
    private static final Pattern MODULE_REPOSITORY_IMPORT = Pattern.compile(
            "import\\s+(com\\.dauducbach\\.clone\\.modules\\.([^.]+)\\.(?:repository|repositoty)\\.[\\w.]+);");
    private static final Set<String> ALLOWED_CROSS_MODULE_REPOSITORY_IMPORTS = Set.of(
            "modules/post/service/story/StoryMediaService.java -> com.dauducbach.clone.modules.user.repositoty.UserDetailsRepository",
            "modules/post/service/story/StoryTrayQueryService.java -> com.dauducbach.clone.modules.media.repositoty.music.MusicsRepository",
            "modules/user/service/MediaForProfile.java -> com.dauducbach.clone.modules.media.repositoty.music.MusicsRepository");
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
    void authenticationControllerDoesNotLogResolvedTokenRequests() {
        String content = read(MAIN_SOURCE.resolve("modules/auth/controller/AuthenticationController.java"));
        assertTrue(!content.contains("Resolved refresh token request"),
                "AuthenticationController must not log requests containing refresh tokens");
    }

    @Test
    void modulesOutsideMediaUseMediaCompatibilityBoundary() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE.resolve("modules"))
                .filter(source -> !source.relativePath().startsWith("modules/media/"))
                .filter(source -> source.content().contains(
                        "import com.dauducbach.clone.modules.media.service.Cloudinary"))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(), () -> "Modules import a vendor-specific Media adapter: " + violations);
    }

    @Test
    void codeOutsideMediaDoesNotUseCloudinarySdkOrUtility() throws IOException {
        List<String> violations = javaSources(MAIN_SOURCE)
                .filter(source -> !source.relativePath().startsWith("modules/media/"))
                .filter(source -> source.content().contains("import com.cloudinary.")
                        || source.content().contains("import com.dauducbach.clone.modules.media.service.Cloudinary"))
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
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(), () -> "Kafka listeners acknowledge before reactive processing completes: " + violations);
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
                            String dependency = source.relativePath() + " -> " + matcher.group(1);
                            if (!ALLOWED_CROSS_MODULE_REPOSITORY_IMPORTS.contains(dependency)) {
                                matches.add(dependency);
                            }
                        }
                    }
                    return matches.stream();
                })
                .toList();

        assertTrue(violations.isEmpty(), () -> "Cross-module repository imports: " + violations);
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
                .filter(source -> source.content().contains("modules.user.repositoty")
                        || source.content().contains("modules.user.repository")
                        || source.content().contains("modules.user.entity"))
                .map(SourceFile::relativePath)
                .toList();

        assertTrue(violations.isEmpty(), () -> "Chat imports User/Story persistence internals: " + violations);
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
                "modules/post/repositoty/PostDetailsRepository.java"));
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
