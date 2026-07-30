package com.dauducbach.clone.modules.post.repositoty;

import com.dauducbach.clone.modules.post.elastic.PostVector;
import org.springframework.data.elasticsearch.repository.ReactiveElasticsearchRepository;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.stereotype.Repository;

@Repository
@NoRepositoryBean
public interface PostVectorRepository extends ReactiveElasticsearchRepository<PostVector, String> {
}

