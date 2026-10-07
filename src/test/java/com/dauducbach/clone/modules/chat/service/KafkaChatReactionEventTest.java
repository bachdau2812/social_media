package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.modules.chat.constant.ReactionType;
import com.dauducbach.clone.modules.chat.publicapi.ChatEvent;
import com.dauducbach.clone.modules.chat.dto.response.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.reactivestreams.Publisher;
import org.springframework.kafka.annotation.KafkaListener;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.*;
import reactor.test.StepVerifier;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class KafkaChatReactionEventTest {
    ChatEvent event() {
        return ChatEvent.reactionChanged("c", new ReactionState("m", 5, 3, "me", ReactionType.HEART,
                2, List.of(new ReactionCount(ReactionType.HEART, 2))), List.of("me", "other"));
    }

    @Test void reactionUsesDedicatedTopicAndNeutralPayload() throws Exception {
        KafkaSender<String, String> sender = mock(KafkaSender.class);
        ObjectMapper mapper = new ObjectMapper();
        when(sender.send(any())).thenReturn(Flux.empty());
        StepVerifier.create(new KafkaChatEventPublisher(sender, mapper).publish(event())).verifyComplete();
        ArgumentCaptor<Publisher<SenderRecord<String, String, String>>> captured = ArgumentCaptor.forClass(Publisher.class);
        verify(sender).send(captured.capture());
        StepVerifier.create(Flux.from(captured.getValue())).assertNext(record -> {
            assertThat(record.topic()).isEqualTo("chat.message.reaction.changed");
            assertThat(record.key()).isEqualTo("c");
            assertThat(record.value()).contains("\"reactionState\"", "\"reactionVersion\":3");
            assertThat(record.value()).doesNotContain("myReaction", "isReact");
        }).verifyComplete();
        var annotation = ChatRealtimeEventListener.class
                .getMethod("handle", org.apache.kafka.clients.consumer.ConsumerRecord.class)
                .getAnnotation(KafkaListener.class);
        assertThat(annotation.topics()).contains("chat.message.reaction.changed");
    }

    @Test void senderResultFailurePropagatesForOutboxRetry() {
        KafkaSender<String, String> sender = mock(KafkaSender.class);
        SenderResult<String> result = mock(SenderResult.class);
        when(result.exception()).thenReturn(new IllegalStateException("Kafka unavailable"));
        when(sender.<String>send(any())).thenReturn(Flux.just(result));
        StepVerifier.create(new KafkaChatEventPublisher(sender, new ObjectMapper()).publish(event()))
                .expectError().verify();
    }

    @Test void pinsUseMutationTopicWithoutNewMessagePayload() {
        KafkaSender<String,String> sender=mock(KafkaSender.class);
        when(sender.send(any())).thenReturn(Flux.empty());
        StepVerifier.create(new KafkaChatEventPublisher(sender,new ObjectMapper()).publish(ChatEvent.pinsChanged("c","me",7,List.of("peer")))).verifyComplete();
        ArgumentCaptor<Publisher<SenderRecord<String,String,String>>> captured=ArgumentCaptor.forClass(Publisher.class);
        verify(sender).send(captured.capture());
        StepVerifier.create(Flux.from(captured.getValue())).assertNext(record->{
            assertThat(record.topic()).isEqualTo("chat.message.mutation");
            assertThat(record.value()).contains("\"pinVersion\":7","\"message\":null");
        }).verifyComplete();
    }

    @Test void newMessageBroadcastNeverCarriesSendersPersonalSelection() {
        ChatMessageResponse message = new ChatMessageResponse("m", "c", 5, "client", "me", "Me", null,
                com.dauducbach.clone.modules.chat.constant.MessageType.TEXT, "hello", null, null, null,
                null, null, false, null, 1, true, ReactionType.HEART,
                List.of(new ReactionCount(ReactionType.HEART, 1)), 2);
        var broadcast = ChatEvent.messageCreated(message, List.of("other")).message();
        assertThat(broadcast.myReaction()).isNull();
        assertThat(broadcast.isReact()).isFalse();
        assertThat(broadcast.likeCount()).isEqualTo(1);
        assertThat(broadcast.reactionVersion()).isEqualTo(2);
    }
}
