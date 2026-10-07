package com.dauducbach.clone.modules.personalization.infrastructure.elasticsearch;

import com.dauducbach.clone.modules.personalization.model.UserDetailVector;
import org.springframework.data.elasticsearch.repository.ReactiveElasticsearchRepository;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.stereotype.Repository;

@Repository
@NoRepositoryBean
public interface UserDetailVectorRepository extends ReactiveElasticsearchRepository<UserDetailVector, String> {
}

