package com.dauducbach.clone.modules.auth.repository;

import com.dauducbach.clone.modules.auth.entity.RefreshTokens;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface RefreshTokensRepository extends ReactiveCrudRepository<RefreshTokens, String> {

}
