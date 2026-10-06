package com.dauducbach.clone.modules.chat.service;
import com.dauducbach.clone.modules.chat.dto.response.ChatMessageResponse;
import com.dauducbach.clone.modules.chat.repository.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.*;
import java.util.List;

@Service @RequiredArgsConstructor(onConstructor_ = @org.springframework.beans.factory.annotation.Autowired)
public class ChatMessageStateService {
    private final ChatMessageAccess access;
    private final ChatMessageRepository messages;
    private final ChatReadRepository reads;
    private final ChatResponseMapper mapper;
    private final TransactionalOperator tx;
    private final ChatMessageQueryService hydration;
    public ChatMessageStateService(ChatMessageAccess access,ChatMessageRepository messages,ChatReadRepository reads,
            ChatResponseMapper mapper,TransactionalOperator tx) { this(access,messages,reads,mapper,tx,null); }
    public Mono<List<ChatMessageResponse>> get(String actor,String c,List<String> ids) {
        if(ids==null||ids.size()>100||ids.stream().anyMatch(id->id==null||id.isBlank()))return Mono.error(ChatMessageAccess.invalid("Supply at most 100 message IDs"));
        return tx.transactional(Mono.defer(()->access.context(actor,c,false).flatMap(ctx->Flux.fromIterable(ids.stream().distinct().toList())
            .concatMap(id->messages.findById(id).filter(m->access.visible(ctx,m)).switchIfEmpty(Mono.error(ChatMessageAccess.forbidden()))
                .flatMap(m->reads.findAfterSequence(c,ctx.visibleFrom(),m.getMessageSeq()-1,1).next().filter(row->row.getId().equals(id))
                    .switchIfEmpty(Mono.error(ChatMessageAccess.forbidden()))).map(mapper::toChatMessageResponse))
            .collectList().flatMap(items->hydration==null?Mono.just(items):hydration.hydrateMessages(actor,c,items)))));
    }
}
