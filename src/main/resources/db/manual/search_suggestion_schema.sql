CREATE TABLE IF NOT EXISTS user_search_histories (
    id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL,
    keyword VARCHAR(255) NOT NULL,
    normalized_keyword VARCHAR(255) NOT NULL,
    search_count BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_searched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_user_search_keyword (user_id, normalized_keyword),
    INDEX idx_user_search_recent (user_id, last_searched_at),
    INDEX idx_user_search_prefix (user_id, normalized_keyword)
);

CREATE TABLE IF NOT EXISTS search_keywords (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    keyword VARCHAR(255) NOT NULL,
    normalized_keyword VARCHAR(255) NOT NULL UNIQUE,
    search_count BIGINT NOT NULL DEFAULT 1,
    user_count BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_search_keywords_prefix (normalized_keyword),
    INDEX idx_search_keywords_count (search_count)
);
