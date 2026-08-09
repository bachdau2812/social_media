package com.dauducbach.clone.modules.post.controller.story;

import com.dauducbach.clone.modules.post.service.story.StoryLibraryService;
import com.dauducbach.clone.modules.post.service.story.StoryReactionService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.core.Authentication;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StoryLibraryControllerTest {
    @Mock StoryLibraryService storyLibraryService;
    @Mock StoryReactionService storyReactionService;
    @Mock Authentication authentication;

    @Test
    void likeStoryLogsRequestAndCompletionWithStableIds() {
        when(authentication.getName()).thenReturn("actor-1");
        when(storyReactionService.like("story-1", "actor-1")).thenReturn(Mono.just(true));
        StoryLibraryController controller = new StoryLibraryController(storyLibraryService, storyReactionService);

        try (LogCapture logs = LogCapture.start()) {
            StepVerifier.create(controller.likeStory("story-1", authentication))
                    .assertNext(response -> assertThat(response.getResult()).isTrue())
                    .verifyComplete();

            assertThat(logs.messages()).anyMatch(message -> message.contains(
                    "|StoryLibraryController|likeStory|requested|storyId=story-1|actorId=actor-1"));
            assertThat(logs.messages()).anyMatch(message -> message.contains(
                    "|StoryLibraryController|likeStory|completed|storyId=story-1|actorId=actor-1|changed=true"));
        }
    }

    @Test
    void likeStoryLogsFailureAndPropagatesTheOriginalError() {
        IllegalStateException failure = new IllegalStateException("outbox unavailable");
        when(authentication.getName()).thenReturn("actor-1");
        when(storyReactionService.like("story-1", "actor-1")).thenReturn(Mono.error(failure));
        StoryLibraryController controller = new StoryLibraryController(storyLibraryService, storyReactionService);

        try (LogCapture logs = LogCapture.start()) {
            StepVerifier.create(controller.likeStory("story-1", authentication))
                    .expectErrorMatches(error -> error == failure)
                    .verify();

            assertThat(logs.messages()).anyMatch(message -> message.contains(
                    "|StoryLibraryController|likeStory|failed|storyId=story-1|actorId=actor-1|errorType=IllegalStateException"));
        }
    }

    private static final class LogCapture implements AutoCloseable {
        private final Logger logger;
        private final CapturingAppender appender;

        private LogCapture() {
            logger = (Logger) LogManager.getLogger(StoryLibraryController.class);
            appender = new CapturingAppender();
            appender.start();
            logger.addAppender(appender);
        }

        static LogCapture start() {
            return new LogCapture();
        }

        List<String> messages() {
            return List.copyOf(appender.messages);
        }

        @Override
        public void close() {
            logger.removeAppender(appender);
            appender.stop();
        }
    }

    private static final class CapturingAppender extends AbstractAppender {
        private final List<String> messages = new CopyOnWriteArrayList<>();

        private CapturingAppender() {
            super("StoryLibraryControllerTestCapture", null,
                    PatternLayout.createDefaultLayout(), false, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
        }
    }
}
