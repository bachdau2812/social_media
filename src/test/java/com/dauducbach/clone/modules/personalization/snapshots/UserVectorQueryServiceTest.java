package com.dauducbach.clone.modules.personalization.snapshots;

import com.dauducbach.clone.modules.personalization.model.UserDetailVector;
import com.dauducbach.clone.modules.personalization.snapshots.UserVectorQueryService;
import org.junit.jupiter.api.Test;
import org.springframework.data.elasticsearch.core.ReactiveElasticsearchOperations;
import org.springframework.data.elasticsearch.core.convert.MappingElasticsearchConverter;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.data.elasticsearch.core.mapping.SimpleElasticsearchMappingContext;
import org.springframework.data.elasticsearch.core.query.SeqNoPrimaryTerm;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

import static org.mockito.Mockito.*;

class UserVectorQueryServiceTest {
    private final ReactiveElasticsearchOperations operations = mock(ReactiveElasticsearchOperations.class);
    private final UserVectorQueryService service = new UserVectorQueryService(operations);

    @Test
    void snapshotExposesDocumentAndDoesNotMaskMissingOrInfrastructureFailure() {
        UserDetailVector document = UserDetailVector.builder().userId("user").build();
        when(operations.get("user", UserDetailVector.class)).thenReturn(Mono.just(document));
        StepVerifier.create(snapshot("user")).expectNext(document).verifyComplete();
        when(operations.get("user", UserDetailVector.class)).thenReturn(Mono.empty());
        StepVerifier.create(snapshot("user")).verifyComplete();
        IllegalStateException failure = new IllegalStateException("ES disconnected");
        when(operations.get("user", UserDetailVector.class)).thenReturn(Mono.error(failure));
        StepVerifier.create(snapshot("user")).expectErrorMatches(error -> error == failure).verify();
    }

    @Test
    void entityCarriesSeparateMetadataHistoryVersionsAndConcurrencyTokens() throws Exception {
        for (String field : List.of("userVectorModel", "userVectorDimension", "userVectorSchemaVersion",
                "userLongTermVectorModel", "userLongTermVectorDimension", "userLongTermVectorSchemaVersion",
                "hasLearnedHistory", "profileVersion", "longTermVersion", "vectorVersion", "seqNoPrimaryTerm")) {
            assertThatCode(() -> UserDetailVector.class.getDeclaredField(field)).doesNotThrowAnyException();
        }
    }

    @Test
    void compatibleLongTermBaseUsesValidatedLongTermVectorBeforeProfileFallback() {
        UserDetailVector document = UserDetailVector.builder()
                .userVector(unitVector(1)).userVectorModel("gemini-embedding-2").userVectorDimension(768).userVectorSchemaVersion(1)
                .userLongTermVector(unitVector(0)).userLongTermVectorModel("gemini-embedding-2").userLongTermVectorDimension(768).userLongTermVectorSchemaVersion(1)
                .build();

        assertThat(service.compatibleLongTermBase(document)).isEqualTo(unitVector(0));

        document.setUserLongTermVector(List.of());
        assertThat(service.compatibleLongTermBase(document)).isEqualTo(unitVector(1));
    }

    @Test
    void compatibleLongTermBaseRejectsUnknownMetadataInsteadOfResettingHistory() {
        UserDetailVector document = UserDetailVector.builder().userLongTermVector(List.of(1.0)).build();

        assertThatThrownBy(() -> service.compatibleLongTermBase(document))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("repair required");
    }

    private List<Double> unitVector(int coordinate) {
        java.util.ArrayList<Double> values = new java.util.ArrayList<>(java.util.Collections.nCopies(768, 0.0));
        values.set(coordinate, 1.0);
        return values;
    }

    private Mono<UserDetailVector> snapshot(String userId) {
        return service.getSnapshot(userId);
    }

    @Test
    void springConverterReadsConcurrencyAndKeepsItOutOfSource() {
        MappingElasticsearchConverter converter = new MappingElasticsearchConverter(new SimpleElasticsearchMappingContext());
        converter.afterPropertiesSet();
        Document source = Document.from(Map.of("user_id", "user", "user_vector_model", "gemini-embedding-2",
                "user_vector_dimension", 768, "user_vector_schema_version", 1,
                "user_long_term_vector_model", "gemini-embedding-2", "user_long_term_vector_dimension", 768,
                "user_long_term_vector_schema_version", 1, "has_learned_history", true, "vector_version", 12L));
        source.setSeqNo(3_000_000_000L);
        source.setPrimaryTerm(2);
        UserDetailVector document = converter.read(UserDetailVector.class, source);
        assertThat(document.getSeqNoPrimaryTerm()).isEqualTo(new SeqNoPrimaryTerm(3_000_000_000L, 2));
        assertThat(document.getUserVectorModel()).isEqualTo("gemini-embedding-2");
        assertThat(document.getUserLongTermVectorSchemaVersion()).isEqualTo(1);
        assertThat(document.getHasLearnedHistory()).isTrue();
        assertThat(document.getVectorVersion()).isEqualTo(12);
        Document serialized = Document.create();
        converter.write(document, serialized);
        assertThat(serialized).doesNotContainKeys("seqNoPrimaryTerm", "seq_no_primary_term");
        assertThat(serialized).containsEntry("has_learned_history", true);
    }

    @Test
    void nullLongTermFieldIsMissing() {
        when(operations.get("user", UserDetailVector.class)).thenReturn(Mono.just(UserDetailVector.builder().build()));
        StepVerifier.create(service.getLongTermVector("user")).expectNext(List.of()).verifyComplete();
    }

    @Test
    void nullProfileFieldIsMissing() {
        when(operations.get("user", UserDetailVector.class)).thenReturn(Mono.just(UserDetailVector.builder().build()));
        StepVerifier.create(service.getUserVector("user")).expectNext(List.of()).verifyComplete();
    }

    @Test
    void missingDocumentIsMissing() {
        when(operations.get("user", UserDetailVector.class)).thenReturn(Mono.empty());
        StepVerifier.create(service.getLongTermVector("user")).expectNext(List.of()).verifyComplete();
        StepVerifier.create(service.getUserVector("user")).expectNext(List.of()).verifyComplete();
    }

    @Test
    void infrastructureFailureDoesNotBecomeMissingLongTerm() {
        IllegalStateException failure = new IllegalStateException("ES unavailable");
        when(operations.get("user", UserDetailVector.class)).thenReturn(Mono.error(failure));
        StepVerifier.create(service.getLongTermVector("user")).expectErrorMatches(error -> error == failure).verify();
    }

    @Test
    void infrastructureFailureDoesNotBecomeMissingProfile() {
        IllegalStateException failure = new IllegalStateException("ES unavailable");
        when(operations.get("user", UserDetailVector.class)).thenReturn(Mono.error(failure));
        StepVerifier.create(service.getUserVector("user")).expectErrorMatches(error -> error == failure).verify();
    }

    @Test
    void fallbackReadsProfileOnlyWhenLongTermMissing() {
        when(operations.get("user", UserDetailVector.class)).thenReturn(Mono.just(UserDetailVector.builder().userVector(List.of(1.0)).build()));
        StepVerifier.create(service.getLongTermOrUserVector("user")).expectNext(List.of(1.0)).verifyComplete();
    }

    @Test
    void blankIdDoesNotReadElasticsearch() {
        StepVerifier.create(service.getLongTermOrUserVector(" ")).expectNext(List.of()).verifyComplete();
        verifyNoInteractions(operations);
    }
}
