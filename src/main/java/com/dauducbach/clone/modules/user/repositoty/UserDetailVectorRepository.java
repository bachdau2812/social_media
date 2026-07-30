package com.dauducbach.clone.modules.user.repositoty;

import com.dauducbach.clone.modules.user.entity.UserDetailVector;
import org.springframework.data.elasticsearch.repository.ReactiveElasticsearchRepository;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.stereotype.Repository;

@Repository
@NoRepositoryBean
public interface UserDetailVectorRepository extends ReactiveElasticsearchRepository<UserDetailVector, String> {
}

