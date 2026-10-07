package com.dauducbach.clone.integration.storyviewers;

import org.junit.jupiter.api.Test;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.RowsFetchSpec;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StoryViewerSearchAdapterTest {
    @Test
    void followingSqlRestrictsBothActorAndPageCandidates() throws Exception {
        String sql = com.dauducbach.clone.modules.user.repository.UserFollowerRepository.class
                .getMethod("findFollowingIds", String.class, java.util.Collection.class)
                .getAnnotation(org.springframework.data.r2dbc.repository.Query.class).value();
        try (var db = DriverManager.getConnection("jdbc:h2:mem:viewer_following_" + UUID.randomUUID() + ";MODE=MySQL")) {
            db.createStatement().execute("CREATE TABLE user_follower(follower_id VARCHAR(64), following_id VARCHAR(64))");
            db.createStatement().execute("INSERT INTO user_follower VALUES ('owner','a'),('owner','outside-page'),('another','b')");
            try (var statement = db.createStatement(); var rows = statement.executeQuery(
                    sql.replace(":followerId", "'owner'").replace(":candidateIds", "'a','b'"))) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString("following_id")).isEqualTo("a");
                assertThat(rows.next()).isFalse();
            }
        }
    }
    @Test
    void filtersBeforePagingCountsMatchesAndUsesStableOrdering() throws Exception {
        try (var db = DriverManager.getConnection("jdbc:h2:mem:story_viewers_" + UUID.randomUUID() + ";MODE=MySQL")) {
            db.createStatement().execute("CREATE TABLE story_views(story_id VARCHAR(64), viewer_id VARCHAR(64), reaction VARCHAR(64), viewed_at TIMESTAMP)");
            db.createStatement().execute("CREATE TABLE user_details(user_id VARCHAR(64), username VARCHAR(64), full_name VARCHAR(255))");
            db.createStatement().execute("INSERT INTO user_details VALUES ('a','mai_one','Mai Hoa'),('b','another','Mai Anh'),('c','unrelated','Other'),('d','100%_real','Percent')");
            db.createStatement().execute("INSERT INTO story_views VALUES ('story','c',NULL,'2026-10-07 12:00:00'),('story','a','LIKE','2026-10-07 11:00:00'),('story','b',NULL,'2026-10-07 11:00:00'),('elsewhere','b',NULL,'2026-10-07 10:00:00'),('story','d',NULL,'2026-10-07 09:00:00')");
            List<String> sql = captureSql("mai", 1, 1);
            String pageSql = sql.stream().filter(query -> query.contains("LIMIT")).findFirst().orElseThrow();
            String countSql = sql.stream().filter(query -> query.contains("COUNT(*)")).findFirst().orElseThrow();
            try (var page = db.prepareStatement(bind(pageSql, "mai", 1, 1)); var rows = page.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString("viewer_id")).isEqualTo("b");
                assertThat(rows.next()).isFalse();
            }
            try (var count = db.prepareStatement(bind(countSql, "mai", 1, 1)); var rows = count.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getLong(1)).isEqualTo(2);
            }
            String literalSql = captureSql("%_", 20, 0).stream().filter(query -> query.contains("LIMIT")).findFirst().orElseThrow();
            try (var query = db.prepareStatement(bind(literalSql, "%_", 20, 0)); var rows = query.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString("viewer_id")).isEqualTo("d");
                assertThat(rows.next()).isFalse();
            }
        }
    }

    private String bind(String sql, String query, int limit, int offset) {
        return sql.replace(":storyId", "'story'").replace(":query", "'" + query + "'")
                .replace(":limit", String.valueOf(limit)).replace(":offset", String.valueOf(offset));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private List<String> captureSql(String query, int limit, int offset) {
        List<String> statements = new ArrayList<>();
        DatabaseClient client = mock(DatabaseClient.class);
        when(client.sql(anyString())).thenAnswer(invocation -> {
            statements.add(invocation.getArgument(0));
            var spec = mock(DatabaseClient.GenericExecuteSpec.class);
            RowsFetchSpec rows = mock(RowsFetchSpec.class);
            when(spec.bind(anyString(), any())).thenReturn(spec);
            when(spec.map(any(BiFunction.class))).thenReturn(rows);
            when(rows.all()).thenReturn(Flux.empty());
            when(rows.one()).thenReturn(Mono.just(0L));
            return spec;
        });
        new StoryViewerSearchAdapter(client).search("story", query, 1, limit, offset).block();
        return statements;
    }
}