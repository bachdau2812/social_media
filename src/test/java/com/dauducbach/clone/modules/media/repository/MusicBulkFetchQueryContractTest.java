package com.dauducbach.clone.modules.media.repository;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class MusicBulkFetchQueryContractTest {
    private static final Path REPOSITORY = Path.of(
            "src/main/java/com/dauducbach/clone/modules/media/repository/MusicsRepository.java");

    @Test
    void exposesTopAndArtistQueriesForOnlyUnfetchedMusic() throws Exception {
        String source = Files.readString(REPOSITORY);

        assertThat(source).contains("findTopUnfetched(Pageable pageable)")
                .contains("findUnfetchedByArtist(String artist, Pageable pageable)")
                .contains("COALESCE(fetched, 0) = 0")
                .contains("LOWER(TRIM(single_name)) = LOWER(TRIM(:artist))")
                .contains("ORDER BY popularity DESC")
                .contains("LIMIT :#{#pageable.pageSize} OFFSET :#{#pageable.offset}");
    }
}
