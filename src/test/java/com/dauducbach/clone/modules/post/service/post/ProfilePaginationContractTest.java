package com.dauducbach.clone.modules.post.service.post;
import com.dauducbach.clone.modules.post.publicapi.*;
import com.dauducbach.clone.modules.post.query.PostContentQueryService;
import com.dauducbach.clone.modules.post.repository.SavedItemRepository;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.user.publicapi.*;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.*;
import reactor.test.StepVerifier;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
class ProfilePaginationContractTest {
 @Test void serializesFlatSeedAndSavedFlag() throws Exception {
  var post=new com.dauducbach.clone.modules.frontend.dto.ProfilePostResponse("p","u","name","Name","avatar","text",java.util.List.of(),"4:3",null,null,7,8,9,true,true,null,null);
  var row=new com.dauducbach.clone.modules.frontend.dto.ProfilePostsPageResponse.TimelinePostResponse(post,true);
  var json=new com.fasterxml.jackson.databind.ObjectMapper().valueToTree(row);
  assertEquals("p",json.get("postId").asText()); assertTrue(json.get("savedByCurrentUser").asBoolean());
  assertTrue(json.get("likedByCurrentUser").asBoolean()); assertEquals(7,json.get("likeCount").asLong()); assertFalse(json.has("post"));
 }
 @Test void exposesPaginatedAuthorContract() throws Exception {
  assertNotNull(PostProfileQuery.class.getMethod("getPostsPage", String.class,String.class,int.class,int.class,String.class));
 }
 @Test void resolvesOldAnchorPageAndPreservesViewerTruth() {
  var content=mock(PostContentQueryService.class); var detail=mock(PostDetailQueryService.class);
  var interactions=mock(PostInteractionQuery.class); var identities=mock(UserIdentityQuery.class);
  var saves=mock(SavedItemRepository.class);
  var service=new PostProfileQueryService(content,detail,mock(RepostService.class),interactions,identities,saves);
  var post=PostDetails.builder().postId("old").userId("owner").content("old post").mediaRatio("9:16").build();
  when(content.findAuthorPostPosition("owner","old")).thenReturn(Mono.just(55L));
  when(content.findByAuthorId("owner",3,18)).thenReturn(Flux.just(post));
  when(content.findByAuthorId("owner",4,18)).thenReturn(Flux.empty());
  when(identities.resolveIdentity("owner")).thenReturn(Mono.just(new UserIdentity("owner","name","Name","avatar")));
  when(detail.getFirstItem(any(),any())).thenReturn(Mono.empty());
  when(detail.getMusicResponse(any(),any(),any())).thenReturn(Mono.empty());
  when(interactions.findSnapshot("old","viewer")).thenReturn(Mono.just(new PostInteractionQuery.Snapshot(7,8,9,true,true)));
  when(saves.existsByUserIdAndPostId("viewer","old")).thenReturn(Mono.just(true));
  StepVerifier.create(service.getPostsPage("viewer","owner",0,18,"old")).assertNext(page->{
   assertEquals(3,page.pageNumber()); assertEquals(18,page.pageSize()); assertTrue(page.hasPrevious());
   assertFalse(page.hasMore()); assertTrue(page.selectedPostFound()); assertTrue(page.posts().getFirst().savedByCurrentUser());
   assertEquals(7,page.posts().getFirst().post().likeCount()); assertTrue(page.posts().getFirst().post().likedByCurrentUser());
   assertTrue(page.posts().getFirst().post().repostedByCurrentUser()); assertEquals("9:16",page.posts().getFirst().post().mediaRatio());
  }).verifyComplete();
  verify(content,never()).findByAuthorId("owner",0,18);
  when(interactions.findSnapshot("old","viewer")).thenReturn(Mono.error(new IllegalStateException("counts unavailable")));
  StepVerifier.create(service.getPostsPage("viewer","owner",0,18,"old")).expectErrorMessage("counts unavailable").verify();
 }
 @Test void missingSelectionReturnsRequestedEmptyPage() {
  var content=mock(PostContentQueryService.class);
  var service=new PostProfileQueryService(content,mock(PostDetailQueryService.class),mock(RepostService.class),mock(PostInteractionQuery.class),mock(UserIdentityQuery.class),mock(SavedItemRepository.class));
  when(content.findAuthorPostPosition("owner","missing")).thenReturn(Mono.empty());
  when(content.findByAuthorId("owner",2,50)).thenReturn(Flux.empty());
  when(content.findByAuthorId("owner",3,50)).thenReturn(Flux.empty());
  StepVerifier.create(service.getPostsPage("viewer","owner",2,200,"missing")).assertNext(page->{
   assertEquals(2,page.pageNumber()); assertEquals(50,page.pageSize()); assertFalse(page.selectedPostFound());
   assertFalse(page.hasMore()); assertTrue(page.posts().isEmpty());
  }).verifyComplete();
 }
}
