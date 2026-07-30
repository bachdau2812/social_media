CREATE TABLE IF NOT EXISTS story_views (
    id VARCHAR(36) NOT NULL PRIMARY KEY,
    story_id VARCHAR(36) NOT NULL,
    viewer_id VARCHAR(36) NOT NULL,
    reaction VARCHAR(32) NULL,
    viewed_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    CONSTRAINT uk_story_views_story_viewer UNIQUE (story_id, viewer_id),
    INDEX idx_story_views_story_time (story_id, viewed_at DESC),
    CONSTRAINT fk_story_views_story FOREIGN KEY (story_id) REFERENCES user_stories(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS story_highlights (
    id VARCHAR(36) NOT NULL PRIMARY KEY,
    owner_id VARCHAR(36) NOT NULL,
    title VARCHAR(120) NOT NULL,
    cover_story_id VARCHAR(36) NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX idx_story_highlights_owner_time (owner_id, updated_at DESC),
    CONSTRAINT fk_story_highlights_cover FOREIGN KEY (cover_story_id) REFERENCES user_stories(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS story_highlight_items (
    id VARCHAR(36) NOT NULL PRIMARY KEY,
    highlight_id VARCHAR(36) NOT NULL,
    story_id VARCHAR(36) NOT NULL,
    order_number INT NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    CONSTRAINT uk_story_highlight_story UNIQUE (highlight_id, story_id),
    INDEX idx_story_highlight_items_order (highlight_id, order_number),
    CONSTRAINT fk_story_highlight_items_highlight FOREIGN KEY (highlight_id) REFERENCES story_highlights(id) ON DELETE CASCADE,
    CONSTRAINT fk_story_highlight_items_story FOREIGN KEY (story_id) REFERENCES user_stories(id) ON DELETE CASCADE
);
