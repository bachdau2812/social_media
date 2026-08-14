package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.user.repositoty.ChatUserSuggestionRepository;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ChatUserSuggestionServiceTest {
    @Test
    void passesPrefixPatternToRepositoryAndKeepsEmptyQueryEmpty() {
        ChatUserSuggestionRepository repository = mock(ChatUserSuggestionRepository.class);
        ChatUserSuggestionService service = new ChatUserSuggestionService(repository);
        when(repository.findSuggestions("user-1", "bach%", 20)).thenReturn(Flux.empty());
        when(repository.findSuggestions("user-1", "", 30)).thenReturn(Flux.empty());

        StepVerifier.create(service.getSuggestions(" user-1 ", " bach ", 20)).verifyComplete();
        StepVerifier.create(service.getSuggestions("user-1", "  ", 0)).verifyComplete();

        verify(repository).findSuggestions("user-1", "bach%", 20);
        verify(repository).findSuggestions("user-1", "", 30);
    }
}
