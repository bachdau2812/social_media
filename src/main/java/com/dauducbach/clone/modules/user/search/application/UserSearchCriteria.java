package com.dauducbach.clone.modules.user.search.application;

public record UserSearchCriteria(
        String queryPattern,
        boolean includeHobby,
        boolean includeLivingIn,
        boolean includeHometown,
        boolean includeSex,
        int page,
        int size
) { }
