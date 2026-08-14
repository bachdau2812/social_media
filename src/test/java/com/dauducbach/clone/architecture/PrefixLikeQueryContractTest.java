package com.dauducbach.clone.architecture;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

class PrefixLikeQueryContractTest {
    private static final Path MAIN_SOURCE = Path.of("src/main/java/com/dauducbach/clone");
    private static final List<Path> SEARCH_REPOSITORIES = List.of(
            MAIN_SOURCE.resolve("modules/media/repositoty/music/MusicsRepository.java"),
            MAIN_SOURCE.resolve("modules/post/repositoty/PostDetailsRepository.java"),
            MAIN_SOURCE.resolve("modules/user/repositoty/UserDetailsRepository.java"),
            MAIN_SOURCE.resolve("modules/user/repositoty/SearchKeywordRepository.java"),
            MAIN_SOURCE.resolve("modules/user/repositoty/ChatUserSuggestionRepository.java")
    );
    private static final Pattern SQL_SIDE_WILDCARD = Pattern.compile(
            "LIKE\\s+CONCAT\\s*\\(", Pattern.CASE_INSENSITIVE);
    private static final Pattern LEADING_WILDCARD = Pattern.compile(
            "LIKE\\s+['\"]%", Pattern.CASE_INSENSITIVE);
    private static final Pattern WRAPPED_SEARCH_COLUMN = Pattern.compile(
            "(?:LOWER|COALESCE)\\s*\\([^\\n]*\\)\\s+LIKE", Pattern.CASE_INSENSITIVE);

    @Test
    void auditedSearchRepositoriesUseBoundPrefixPatternsOnRawColumns() throws IOException {
        for (Path repository : SEARCH_REPOSITORIES) {
            String sql = Files.readString(repository);

            assertThat(SQL_SIDE_WILDCARD.matcher(sql).find())
                    .as("SQL-side wildcard concatenation in %s", repository)
                    .isFalse();
            assertThat(LEADING_WILDCARD.matcher(sql).find())
                    .as("leading wildcard in %s", repository)
                    .isFalse();
            assertThat(WRAPPED_SEARCH_COLUMN.matcher(sql).find())
                    .as("function-wrapped LIKE column in %s", repository)
                    .isFalse();
        }
    }
}
