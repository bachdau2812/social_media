package com.dauducbach.clone.modules.user.search.infrastructure.config;

import com.dauducbach.clone.modules.user.search.application.SemanticUserSearch;
import com.dauducbach.clone.modules.user.search.application.UserSearchIndex;
import com.dauducbach.clone.modules.user.search.application.UserSearchService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class UserSearchConfiguration {
    @Bean
    UserSearchService userSearchService(UserSearchIndex index, SemanticUserSearch semanticSearch) {
        return new UserSearchService(index, semanticSearch);
    }
}
