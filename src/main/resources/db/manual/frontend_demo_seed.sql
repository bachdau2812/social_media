SET @now = NOW(3);
SET @viewer = '11111111-1111-1111-1111-111111111111';
SET @anna = '22222222-2222-2222-2222-222222222222';
SET @minh = '33333333-3333-3333-3333-333333333333';
SET @linh = '44444444-4444-4444-4444-444444444444';
SET @hash = '$2a$10$IDOJnAFkCefozfz6vH2CFe.6pwSDvTDXvMD6GfJeTbMWVwWmvutU2';

INSERT INTO user_credentials (user_id, username, user_password, user_role, email, provider, provider_id) VALUES
(@viewer, 'codex_fe_test', @hash, 'USER', 'codex.fe.test@example.local', 'LOCAL', NULL),
(@anna, 'anna.nguyen', @hash, 'USER', 'anna.nguyen@example.local', 'LOCAL', NULL),
(@minh, 'minh.tran', @hash, 'USER', 'minh.tran@example.local', 'LOCAL', NULL),
(@linh, 'linh.art', @hash, 'USER', 'linh.art@example.local', 'LOCAL', NULL)
ON DUPLICATE KEY UPDATE username = VALUES(username), user_password = VALUES(user_password), user_role = VALUES(user_role), email = VALUES(email), provider = VALUES(provider);

INSERT INTO user_details (user_id, username, full_name, dob, hometown, living_in, sex, hobby_list) VALUES
(@viewer, 'codex_fe_test', 'Codex FE Test', '1995-01-15', 'Da Nang', 'Ho Chi Minh City', 'OTHER', '["backend","react","photography"]'),
(@anna, 'anna.nguyen', 'Anna Nguyen', '1996-03-21', 'Ha Noi', 'Ho Chi Minh City', 'FEMALE', '["design","coffee","weekend trips"]'),
(@minh, 'minh.tran', 'Minh Tran', '1994-07-08', 'Hue', 'Da Nang', 'MALE', '["street photo","running","music"]'),
(@linh, 'linh.art', 'Linh Artist', '1998-11-02', 'Can Tho', 'Ha Noi', 'FEMALE', '["illustration","books","cinema"]')
ON DUPLICATE KEY UPDATE username = VALUES(username), full_name = VALUES(full_name), dob = VALUES(dob), hometown = VALUES(hometown), living_in = VALUES(living_in), sex = VALUES(sex), hobby_list = VALUES(hobby_list);

INSERT INTO user_follower (id, follower_id, following_id, created_at) VALUES
('flw-seed-0001', @viewer, @anna, @now),
('flw-seed-0002', @anna, @viewer, @now),
('flw-seed-0003', @viewer, @minh, @now),
('flw-seed-0004', @linh, @viewer, @now)
ON DUPLICATE KEY UPDATE follower_id = VALUES(follower_id), following_id = VALUES(following_id), created_at = VALUES(created_at);

INSERT INTO post_details (post_id, user_id, content, hashtag, created_at, updated_at, validate_status) VALUES
('post-seed-viewer-01', @viewer, 'Building the social media frontend against the real Spring WebFlux API. Demo data is ready for profile, feed, and search screens.', '["spring","react","demo"]', DATE_SUB(@now, INTERVAL 5 MINUTE), DATE_SUB(@now, INTERVAL 5 MINUTE), 'APPROVED'),
('post-seed-anna-01', @anna, 'Morning design review with a compact feed layout, sharper actions, and fewer placeholder states.', '["design","frontend","social"]', DATE_SUB(@now, INTERVAL 15 MINUTE), DATE_SUB(@now, INTERVAL 15 MINUTE), 'APPROVED'),
('post-seed-anna-02', @anna, 'Friends tab should feel alive: mutual follow data now powers this post directly from MySQL.', '["friends","feed","demo"]', DATE_SUB(@now, INTERVAL 45 MINUTE), DATE_SUB(@now, INTERVAL 45 MINUTE), 'APPROVED'),
('post-seed-minh-01', @minh, 'Street photography notes from Da Nang: light, movement, and a search keyword for demo testing.', '["photography","danang","search"]', DATE_SUB(@now, INTERVAL 90 MINUTE), DATE_SUB(@now, INTERVAL 90 MINUTE), 'APPROVED'),
('post-seed-linh-01', @linh, 'Sketching a calm notification center and a saved collection workflow for the frontend.', '["illustration","library","alerts"]', DATE_SUB(@now, INTERVAL 150 MINUTE), DATE_SUB(@now, INTERVAL 150 MINUTE), 'APPROVED')
ON DUPLICATE KEY UPDATE user_id = VALUES(user_id), content = VALUES(content), hashtag = VALUES(hashtag), created_at = VALUES(created_at), updated_at = VALUES(updated_at), validate_status = VALUES(validate_status);

INSERT INTO media (asset_id, public_id, width, height, media_format, resource_type, bytes, url, secure_url, owner_id, owner_type, version, version_id, display_name, created_at, updated_at) VALUES
('media-avatar-viewer-01', 'seed/avatar/viewer', 320, 320, 'jpg', 'image', 125000, 'https://picsum.photos/seed/codex-avatar/320/320', 'https://picsum.photos/seed/codex-avatar/320/320', @viewer, 'AVATAR', '1', 'seed', 'Codex avatar', DATE_SUB(@now, INTERVAL 1 DAY), @now),
('media-avatar-anna-01', 'seed/avatar/anna', 320, 320, 'jpg', 'image', 125000, 'https://picsum.photos/seed/anna-avatar/320/320', 'https://picsum.photos/seed/anna-avatar/320/320', @anna, 'AVATAR', '1', 'seed', 'Anna avatar', DATE_SUB(@now, INTERVAL 1 DAY), @now),
('media-avatar-minh-01', 'seed/avatar/minh', 320, 320, 'jpg', 'image', 125000, 'https://picsum.photos/seed/minh-avatar/320/320', 'https://picsum.photos/seed/minh-avatar/320/320', @minh, 'AVATAR', '1', 'seed', 'Minh avatar', DATE_SUB(@now, INTERVAL 1 DAY), @now),
('media-post-viewer-01', 'seed/post/viewer-01', 1200, 900, 'jpg', 'image', 345000, 'https://picsum.photos/seed/social-viewer/1200/900', 'https://picsum.photos/seed/social-viewer/1200/900', 'post-seed-viewer-01', 'POST', '1', 'seed', 'Workspace demo', DATE_SUB(@now, INTERVAL 5 MINUTE), DATE_SUB(@now, INTERVAL 5 MINUTE)),
('media-post-anna-01', 'seed/post/anna-01', 1200, 1500, 'jpg', 'image', 390000, 'https://picsum.photos/seed/social-anna-1/1200/1500', 'https://picsum.photos/seed/social-anna-1/1200/1500', 'post-seed-anna-01', 'POST', '1', 'seed', 'Design board', DATE_SUB(@now, INTERVAL 15 MINUTE), DATE_SUB(@now, INTERVAL 15 MINUTE)),
('media-post-anna-02', 'seed/post/anna-02', 1200, 900, 'jpg', 'image', 360000, 'https://picsum.photos/seed/social-anna-2/1200/900', 'https://picsum.photos/seed/social-anna-2/1200/900', 'post-seed-anna-02', 'POST', '1', 'seed', 'Friends feed', DATE_SUB(@now, INTERVAL 45 MINUTE), DATE_SUB(@now, INTERVAL 45 MINUTE)),
('media-post-minh-01', 'seed/post/minh-01', 1200, 900, 'jpg', 'image', 360000, 'https://picsum.photos/seed/social-minh/1200/900', 'https://picsum.photos/seed/social-minh/1200/900', 'post-seed-minh-01', 'POST', '1', 'seed', 'Street frame', DATE_SUB(@now, INTERVAL 90 MINUTE), DATE_SUB(@now, INTERVAL 90 MINUTE)),
('media-post-linh-01', 'seed/post/linh-01', 1200, 900, 'jpg', 'image', 360000, 'https://picsum.photos/seed/social-linh/1200/900', 'https://picsum.photos/seed/social-linh/1200/900', 'post-seed-linh-01', 'POST', '1', 'seed', 'Sketch desk', DATE_SUB(@now, INTERVAL 150 MINUTE), DATE_SUB(@now, INTERVAL 150 MINUTE))
ON DUPLICATE KEY UPDATE public_id = VALUES(public_id), width = VALUES(width), height = VALUES(height), media_format = VALUES(media_format), resource_type = VALUES(resource_type), bytes = VALUES(bytes), url = VALUES(url), secure_url = VALUES(secure_url), owner_id = VALUES(owner_id), owner_type = VALUES(owner_type), display_name = VALUES(display_name), updated_at = VALUES(updated_at);

INSERT INTO user_stories (id, user_id, media_url, media_type, music_url, music_start, music_end, status, created_at, expired_at) VALUES
('story-seed-viewer-01', @viewer, 'https://picsum.photos/seed/story-viewer/720/1280', 'IMAGE', NULL, NULL, NULL, 'APPROVED', DATE_SUB(@now, INTERVAL 20 MINUTE), DATE_ADD(@now, INTERVAL 23 HOUR)),
('story-seed-anna-01', @anna, 'https://picsum.photos/seed/story-anna/720/1280', 'IMAGE', NULL, NULL, NULL, 'APPROVED', DATE_SUB(@now, INTERVAL 35 MINUTE), DATE_ADD(@now, INTERVAL 22 HOUR)),
('story-seed-minh-01', @minh, 'https://picsum.photos/seed/story-minh/720/1280', 'IMAGE', NULL, NULL, NULL, 'APPROVED', DATE_SUB(@now, INTERVAL 50 MINUTE), DATE_ADD(@now, INTERVAL 21 HOUR))
ON DUPLICATE KEY UPDATE user_id = VALUES(user_id), media_url = VALUES(media_url), media_type = VALUES(media_type), status = VALUES(status), created_at = VALUES(created_at), expired_at = VALUES(expired_at);

INSERT INTO comments (id, post_id, user_id, parent_id, content, comment_type, media_url, timestamp) VALUES
('comment-seed-0001', 'post-seed-anna-01', @viewer, NULL, 'Layout looks clean on desktop and mobile.', 'TEXT', NULL, DATE_SUB(@now, INTERVAL 10 MINUTE)),
('comment-seed-0002', 'post-seed-viewer-01', @anna, NULL, 'The API driven state is much easier to verify now.', 'TEXT', NULL, DATE_SUB(@now, INTERVAL 4 MINUTE))
ON DUPLICATE KEY UPDATE post_id = VALUES(post_id), user_id = VALUES(user_id), content = VALUES(content), comment_type = VALUES(comment_type), timestamp = VALUES(timestamp);

INSERT INTO likes (id, actor_id, target_id, target_type, timestamp) VALUES
('like-seed-0001', @viewer, 'post-seed-anna-01', 'POST', DATE_SUB(@now, INTERVAL 9 MINUTE)),
('like-seed-0002', @anna, 'post-seed-viewer-01', 'POST', DATE_SUB(@now, INTERVAL 3 MINUTE)),
('like-seed-0003', @minh, 'post-seed-anna-02', 'POST', DATE_SUB(@now, INTERVAL 30 MINUTE))
ON DUPLICATE KEY UPDATE actor_id = VALUES(actor_id), target_id = VALUES(target_id), target_type = VALUES(target_type), timestamp = VALUES(timestamp);

INSERT INTO notification_events (id, actor_id, action_type, entity_id, entity_type, created_at) VALUES
('notif-event-seed-01', @anna, 'LIKE', 'post-seed-viewer-01', 'POST', DATE_SUB(@now, INTERVAL 3 MINUTE)),
('notif-event-seed-02', @minh, 'COMMENT', 'post-seed-viewer-01', 'POST', DATE_SUB(@now, INTERVAL 8 MINUTE)),
('notif-event-seed-03', @linh, 'FOLLOW', @viewer, 'USER', DATE_SUB(@now, INTERVAL 20 MINUTE))
ON DUPLICATE KEY UPDATE actor_id = VALUES(actor_id), action_type = VALUES(action_type), entity_id = VALUES(entity_id), entity_type = VALUES(entity_type), created_at = VALUES(created_at);

INSERT INTO user_notifications (id, user_id, event_id, notification_status, read_at, created_at) VALUES
('user-notif-seed-01', @viewer, 'notif-event-seed-01', 'UNREAD', NULL, DATE_SUB(@now, INTERVAL 3 MINUTE)),
('user-notif-seed-02', @viewer, 'notif-event-seed-02', 'UNREAD', NULL, DATE_SUB(@now, INTERVAL 8 MINUTE)),
('user-notif-seed-03', @viewer, 'notif-event-seed-03', 'READ', DATE_SUB(@now, INTERVAL 10 MINUTE), DATE_SUB(@now, INTERVAL 20 MINUTE))
ON DUPLICATE KEY UPDATE user_id = VALUES(user_id), event_id = VALUES(event_id), notification_status = VALUES(notification_status), read_at = VALUES(read_at), created_at = VALUES(created_at);

INSERT INTO saved_collections (id, user_id, name, cover_thumbnail_urls, item_count, created_at, updated_at) VALUES
('collection-seed-01', @viewer, 'API verified posts', '["https://picsum.photos/seed/social-anna-1/360/360","https://picsum.photos/seed/social-minh/360/360"]', 2, DATE_SUB(@now, INTERVAL 1 DAY), @now),
('collection-seed-02', @viewer, 'Design references', '["https://picsum.photos/seed/social-anna-2/360/360","https://picsum.photos/seed/social-linh/360/360"]', 2, DATE_SUB(@now, INTERVAL 2 DAY), DATE_SUB(@now, INTERVAL 20 MINUTE)),
('collection-seed-03', @viewer, 'Weekend ideas', '["https://picsum.photos/seed/social-viewer/360/360"]', 1, DATE_SUB(@now, INTERVAL 3 DAY), DATE_SUB(@now, INTERVAL 40 MINUTE))
ON DUPLICATE KEY UPDATE name = VALUES(name), cover_thumbnail_urls = VALUES(cover_thumbnail_urls), item_count = VALUES(item_count), updated_at = VALUES(updated_at);

INSERT INTO saved_items (id, user_id, post_id, collection_id, created_at) VALUES
('saved-seed-0001', @viewer, 'post-seed-anna-01', 'collection-seed-01', DATE_SUB(@now, INTERVAL 12 MINUTE)),
('saved-seed-0002', @viewer, 'post-seed-minh-01', 'collection-seed-01', DATE_SUB(@now, INTERVAL 60 MINUTE)),
('saved-seed-0003', @viewer, 'post-seed-anna-02', 'collection-seed-02', DATE_SUB(@now, INTERVAL 75 MINUTE)),
('saved-seed-0004', @viewer, 'post-seed-linh-01', 'collection-seed-02', DATE_SUB(@now, INTERVAL 95 MINUTE)),
('saved-seed-0005', @viewer, 'post-seed-viewer-01', 'collection-seed-03', DATE_SUB(@now, INTERVAL 130 MINUTE))
ON DUPLICATE KEY UPDATE user_id = VALUES(user_id), post_id = VALUES(post_id), collection_id = VALUES(collection_id), created_at = VALUES(created_at);

INSERT INTO user_drafts (id, user_id, draft_type, thumbnail_url, media_count, caption_preview, payload, created_at, updated_at) VALUES
('draft-seed-0001', @viewer, 'POST', 'https://picsum.photos/seed/draft-social/360/360', 1, 'Draft from imported frontend demo data', '{"caption":"Draft from seed"}', DATE_SUB(@now, INTERVAL 2 HOUR), @now),
('draft-seed-0002', @viewer, 'STORY', 'https://picsum.photos/seed/draft-story/360/640', 2, 'Story draft with two media items', '{"items":["story-a","story-b"]}', DATE_SUB(@now, INTERVAL 1 DAY), DATE_SUB(@now, INTERVAL 35 MINUTE)),
('draft-seed-0003', @viewer, 'POST', 'https://picsum.photos/seed/draft-carousel/360/360', 4, 'Carousel post draft for Library screen', '{"caption":"Carousel draft","mediaCount":4}', DATE_SUB(@now, INTERVAL 3 DAY), DATE_SUB(@now, INTERVAL 90 MINUTE))
ON DUPLICATE KEY UPDATE draft_type = VALUES(draft_type), thumbnail_url = VALUES(thumbnail_url), media_count = VALUES(media_count), caption_preview = VALUES(caption_preview), payload = VALUES(payload), updated_at = VALUES(updated_at);

INSERT INTO user_archive_items (id, user_id, content_id, content_type, thumbnail_url, caption_preview, archived_at) VALUES
('archive-seed-0001', @viewer, 'post-seed-linh-01', 'POST', 'https://picsum.photos/seed/social-linh/360/360', 'Archived imported demo post', DATE_SUB(@now, INTERVAL 1 DAY)),
('archive-seed-0002', @viewer, 'story-seed-viewer-01', 'STORY', 'https://picsum.photos/seed/story-viewer/360/640', 'Archived story from frontend demo', DATE_SUB(@now, INTERVAL 2 DAY)),
('archive-seed-0003', @viewer, 'post-seed-anna-02', 'POST', 'https://picsum.photos/seed/social-anna-2/360/360', 'Archived friend feed example', DATE_SUB(@now, INTERVAL 4 DAY))
ON DUPLICATE KEY UPDATE content_id = VALUES(content_id), content_type = VALUES(content_type), thumbnail_url = VALUES(thumbnail_url), caption_preview = VALUES(caption_preview), archived_at = VALUES(archived_at);

INSERT INTO user_settings (user_id, account_visibility, story_visibility, comment_permission, mention_permission, tag_approval_required, activity_status_visible, read_receipts_enabled, push_enabled, email_enabled, likes_enabled, comments_enabled, follows_enabled, mentions_enabled, stories_enabled, messages_enabled, security_enabled, sensitive_content_level, autoplay_video, theme, reduced_motion, text_scale, high_contrast, always_show_captions, updated_at) VALUES
(@viewer, 'PUBLIC', 'FOLLOWERS', 'EVERYONE', 'EVERYONE', 0, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 'STANDARD', 'WIFI_ONLY', 'SYSTEM', 0, 1, 0, 0, @now)
ON DUPLICATE KEY UPDATE account_visibility = VALUES(account_visibility), story_visibility = VALUES(story_visibility), comment_permission = VALUES(comment_permission), mention_permission = VALUES(mention_permission), updated_at = VALUES(updated_at);

INSERT INTO conversations (id, conversation_type, title, direct_key, last_message_seq, last_message_id, last_message_at, created_by, created_at, updated_at) VALUES
('conv-seed-0001', 'DIRECT', NULL, REPEAT('c', 64), 2, 'msg-seed-0002', DATE_SUB(@now, INTERVAL 2 MINUTE), @viewer, DATE_SUB(@now, INTERVAL 1 DAY), @now)
ON DUPLICATE KEY UPDATE conversation_type = VALUES(conversation_type), title = VALUES(title), last_message_seq = VALUES(last_message_seq), last_message_id = VALUES(last_message_id), last_message_at = VALUES(last_message_at), updated_at = VALUES(updated_at);

INSERT INTO conversation_members (id, conversation_id, user_id, member_role, member_status, joined_seq, last_delivered_seq, last_read_seq, muted_until, joined_at, left_at) VALUES
('conv-mem-seed-01', 'conv-seed-0001', @viewer, 'USER', 'ACTIVE', 1, 2, 1, NULL, DATE_SUB(@now, INTERVAL 1 DAY), NULL),
('conv-mem-seed-02', 'conv-seed-0001', @anna, 'USER', 'ACTIVE', 1, 2, 2, NULL, DATE_SUB(@now, INTERVAL 1 DAY), NULL)
ON DUPLICATE KEY UPDATE member_role = VALUES(member_role), member_status = VALUES(member_status), last_delivered_seq = VALUES(last_delivered_seq), last_read_seq = VALUES(last_read_seq), left_at = VALUES(left_at);

INSERT INTO messages (id, conversation_id, message_seq, client_message_id, sender_id, message_type, content, metadata, reply_to_seq, created_at, edited_at, deleted_at) VALUES
('msg-seed-0001', 'conv-seed-0001', 1, 'client-seed-0001', @viewer, 'TEXT', 'Seed conversation is ready for chat list testing.', NULL, NULL, DATE_SUB(@now, INTERVAL 5 MINUTE), NULL, NULL),
('msg-seed-0002', 'conv-seed-0001', 2, 'client-seed-0002', @anna, 'TEXT', 'I can see the real backend conversation now.', NULL, NULL, DATE_SUB(@now, INTERVAL 2 MINUTE), NULL, NULL)
ON DUPLICATE KEY UPDATE sender_id = VALUES(sender_id), content = VALUES(content), created_at = VALUES(created_at), edited_at = VALUES(edited_at), deleted_at = VALUES(deleted_at);
INSERT INTO post_details (post_id, user_id, content, hashtag, created_at, updated_at, validate_status) VALUES
('post-seed-ready-01', @anna, 'Fresh seeded post reserved for the next frontend Home load after automated verification.', '["ready","feed","demo"]', DATE_SUB(@now, INTERVAL 1 MINUTE), DATE_SUB(@now, INTERVAL 1 MINUTE), 'APPROVED'),
('post-seed-ready-02', @minh, 'Fresh Discover item kept unconsumed so the feed has visible data when opened manually.', '["discover","api","seed"]', DATE_SUB(@now, INTERVAL 2 MINUTE), DATE_SUB(@now, INTERVAL 2 MINUTE), 'APPROVED'),
('post-seed-ready-03', @viewer, 'Fresh profile and feed item imported after browser audit.', '["profile","frontend","ready"]', DATE_SUB(@now, INTERVAL 3 MINUTE), DATE_SUB(@now, INTERVAL 3 MINUTE), 'APPROVED')
ON DUPLICATE KEY UPDATE user_id = VALUES(user_id), content = VALUES(content), hashtag = VALUES(hashtag), created_at = VALUES(created_at), updated_at = VALUES(updated_at), validate_status = VALUES(validate_status);

INSERT INTO media (asset_id, public_id, width, height, media_format, resource_type, bytes, url, secure_url, owner_id, owner_type, version, version_id, display_name, created_at, updated_at) VALUES
('media-ready-anna-01', 'seed/ready/anna-01', 1200, 900, 'jpg', 'image', 330000, 'https://picsum.photos/seed/ready-anna/1200/900', 'https://picsum.photos/seed/ready-anna/1200/900', 'post-seed-ready-01', 'POST', '1', 'seed', 'Ready friends feed', DATE_SUB(@now, INTERVAL 1 MINUTE), DATE_SUB(@now, INTERVAL 1 MINUTE)),
('media-ready-minh-01', 'seed/ready/minh-01', 1200, 900, 'jpg', 'image', 330000, 'https://picsum.photos/seed/ready-minh/1200/900', 'https://picsum.photos/seed/ready-minh/1200/900', 'post-seed-ready-02', 'POST', '1', 'seed', 'Ready discover feed', DATE_SUB(@now, INTERVAL 2 MINUTE), DATE_SUB(@now, INTERVAL 2 MINUTE)),
('media-ready-viewer-01', 'seed/ready/viewer-01', 1200, 900, 'jpg', 'image', 330000, 'https://picsum.photos/seed/ready-viewer/1200/900', 'https://picsum.photos/seed/ready-viewer/1200/900', 'post-seed-ready-03', 'POST', '1', 'seed', 'Ready profile feed', DATE_SUB(@now, INTERVAL 3 MINUTE), DATE_SUB(@now, INTERVAL 3 MINUTE))
ON DUPLICATE KEY UPDATE public_id = VALUES(public_id), width = VALUES(width), height = VALUES(height), media_format = VALUES(media_format), resource_type = VALUES(resource_type), bytes = VALUES(bytes), url = VALUES(url), secure_url = VALUES(secure_url), owner_id = VALUES(owner_id), owner_type = VALUES(owner_type), display_name = VALUES(display_name), updated_at = VALUES(updated_at);
SET @quang = '55555555-5555-5555-5555-555555555555';
SET @mai = '66666666-6666-6666-6666-666666666666';
SET @bao = '77777777-7777-7777-7777-777777777777';

INSERT INTO user_credentials (user_id, username, user_password, user_role, email, provider, provider_id) VALUES
(@quang, 'quang.dev', @hash, 'USER', 'quang.dev@example.local', 'LOCAL', NULL),
(@mai, 'mai.travel', @hash, 'USER', 'mai.travel@example.local', 'LOCAL', NULL),
(@bao, 'bao.product', @hash, 'USER', 'bao.product@example.local', 'LOCAL', NULL)
ON DUPLICATE KEY UPDATE username = VALUES(username), user_password = VALUES(user_password), user_role = VALUES(user_role), email = VALUES(email), provider = VALUES(provider);

INSERT INTO user_details (user_id, username, full_name, dob, hometown, living_in, sex, hobby_list) VALUES
(@quang, 'quang.dev', 'Quang Dev', '1993-09-13', 'Da Nang', 'Ho Chi Minh City', 'MALE', '["backend","coffee","productivity"]'),
(@mai, 'mai.travel', 'Mai Travel', '1997-05-19', 'Nha Trang', 'Da Nang', 'FEMALE', '["travel","food","photography"]'),
(@bao, 'bao.product', 'Bao Product', '1992-12-04', 'Ho Chi Minh City', 'Ha Noi', 'MALE', '["product","analytics","running"]')
ON DUPLICATE KEY UPDATE username = VALUES(username), full_name = VALUES(full_name), dob = VALUES(dob), hometown = VALUES(hometown), living_in = VALUES(living_in), sex = VALUES(sex), hobby_list = VALUES(hobby_list);

INSERT INTO user_follower (id, follower_id, following_id, created_at) VALUES
('flw-seed-0005', @minh, @viewer, DATE_SUB(@now, INTERVAL 2 HOUR)),
('flw-seed-0006', @viewer, @linh, DATE_SUB(@now, INTERVAL 90 MINUTE)),
('flw-seed-0007', @viewer, @quang, DATE_SUB(@now, INTERVAL 80 MINUTE)),
('flw-seed-0008', @quang, @viewer, DATE_SUB(@now, INTERVAL 70 MINUTE)),
('flw-seed-0009', @viewer, @mai, DATE_SUB(@now, INTERVAL 65 MINUTE)),
('flw-seed-0010', @mai, @viewer, DATE_SUB(@now, INTERVAL 60 MINUTE)),
('flw-seed-0011', @bao, @viewer, DATE_SUB(@now, INTERVAL 55 MINUTE)),
('flw-seed-0012', @anna, @minh, DATE_SUB(@now, INTERVAL 50 MINUTE)),
('flw-seed-0013', @minh, @anna, DATE_SUB(@now, INTERVAL 45 MINUTE)),
('flw-seed-0014', @linh, @mai, DATE_SUB(@now, INTERVAL 40 MINUTE)),
('flw-seed-0015', @mai, @linh, DATE_SUB(@now, INTERVAL 35 MINUTE))
ON DUPLICATE KEY UPDATE follower_id = VALUES(follower_id), following_id = VALUES(following_id), created_at = VALUES(created_at);

INSERT INTO post_details (post_id, user_id, content, hashtag, created_at, updated_at, validate_status) VALUES
('post-seed-quang-01', @quang, 'Pairing on the chat module today: direct conversations, group members, cursor state and message history are all seeded for testing.', '["chat","backend","webflux"]', DATE_SUB(@now, INTERVAL 6 MINUTE), DATE_SUB(@now, INTERVAL 6 MINUTE), 'APPROVED'),
('post-seed-quang-02', @quang, 'Tiny UI details matter: compact nav first, labels on hover, and clean action buttons that keep the feed scannable.', '["ui","frontend","pulse"]', DATE_SUB(@now, INTERVAL 32 MINUTE), DATE_SUB(@now, INTERVAL 32 MINUTE), 'APPROVED'),
('post-seed-mai-01', @mai, 'A sunny Da Nang photo dump for story and profile grid testing. This one should appear in Friends for mutual connections.', '["travel","danang","story"]', DATE_SUB(@now, INTERVAL 12 MINUTE), DATE_SUB(@now, INTERVAL 12 MINUTE), 'APPROVED'),
('post-seed-mai-02', @mai, 'Coffee stop before a product review. Search for coffee or travel to find this seeded post.', '["coffee","travel","search"]', DATE_SUB(@now, INTERVAL 72 MINUTE), DATE_SUB(@now, INTERVAL 72 MINUTE), 'APPROVED'),
('post-seed-bao-01', @bao, 'Product notes: notification states, saved collections and archive screens now have richer seed data.', '["product","notifications","library"]', DATE_SUB(@now, INTERVAL 18 MINUTE), DATE_SUB(@now, INTERVAL 18 MINUTE), 'APPROVED')
ON DUPLICATE KEY UPDATE user_id = VALUES(user_id), content = VALUES(content), hashtag = VALUES(hashtag), created_at = VALUES(created_at), updated_at = VALUES(updated_at), validate_status = VALUES(validate_status);

INSERT INTO media (asset_id, public_id, width, height, media_format, resource_type, bytes, url, secure_url, owner_id, owner_type, version, version_id, display_name, created_at, updated_at) VALUES
('media-avatar-quang-01', 'seed/avatar/quang', 320, 320, 'jpg', 'image', 125000, 'https://picsum.photos/seed/quang-avatar/320/320', 'https://picsum.photos/seed/quang-avatar/320/320', @quang, 'AVATAR', '1', 'seed', 'Quang avatar', DATE_SUB(@now, INTERVAL 1 DAY), @now),
('media-avatar-mai-01', 'seed/avatar/mai', 320, 320, 'jpg', 'image', 125000, 'https://picsum.photos/seed/mai-avatar/320/320', 'https://picsum.photos/seed/mai-avatar/320/320', @mai, 'AVATAR', '1', 'seed', 'Mai avatar', DATE_SUB(@now, INTERVAL 1 DAY), @now),
('media-avatar-bao-01', 'seed/avatar/bao', 320, 320, 'jpg', 'image', 125000, 'https://picsum.photos/seed/bao-avatar/320/320', 'https://picsum.photos/seed/bao-avatar/320/320', @bao, 'AVATAR', '1', 'seed', 'Bao avatar', DATE_SUB(@now, INTERVAL 1 DAY), @now),
('media-post-quang-01', 'seed/post/quang-01', 1200, 900, 'jpg', 'image', 350000, 'https://picsum.photos/seed/social-quang-1/1200/900', 'https://picsum.photos/seed/social-quang-1/1200/900', 'post-seed-quang-01', 'POST', '1', 'seed', 'Chat module desk', DATE_SUB(@now, INTERVAL 6 MINUTE), DATE_SUB(@now, INTERVAL 6 MINUTE)),
('media-post-quang-02', 'seed/post/quang-02', 1200, 1500, 'jpg', 'image', 380000, 'https://picsum.photos/seed/social-quang-2/1200/1500', 'https://picsum.photos/seed/social-quang-2/1200/1500', 'post-seed-quang-02', 'POST', '1', 'seed', 'Compact UI rail', DATE_SUB(@now, INTERVAL 32 MINUTE), DATE_SUB(@now, INTERVAL 32 MINUTE)),
('media-post-mai-01', 'seed/post/mai-01', 1200, 1500, 'jpg', 'image', 390000, 'https://picsum.photos/seed/social-mai-1/1200/1500', 'https://picsum.photos/seed/social-mai-1/1200/1500', 'post-seed-mai-01', 'POST', '1', 'seed', 'Da Nang light', DATE_SUB(@now, INTERVAL 12 MINUTE), DATE_SUB(@now, INTERVAL 12 MINUTE)),
('media-post-mai-02', 'seed/post/mai-02', 1200, 900, 'jpg', 'image', 340000, 'https://picsum.photos/seed/social-mai-2/1200/900', 'https://picsum.photos/seed/social-mai-2/1200/900', 'post-seed-mai-02', 'POST', '1', 'seed', 'Coffee stop', DATE_SUB(@now, INTERVAL 72 MINUTE), DATE_SUB(@now, INTERVAL 72 MINUTE)),
('media-post-bao-01', 'seed/post/bao-01', 1200, 900, 'jpg', 'image', 340000, 'https://picsum.photos/seed/social-bao-1/1200/900', 'https://picsum.photos/seed/social-bao-1/1200/900', 'post-seed-bao-01', 'POST', '1', 'seed', 'Product notes', DATE_SUB(@now, INTERVAL 18 MINUTE), DATE_SUB(@now, INTERVAL 18 MINUTE))
ON DUPLICATE KEY UPDATE public_id = VALUES(public_id), width = VALUES(width), height = VALUES(height), media_format = VALUES(media_format), resource_type = VALUES(resource_type), bytes = VALUES(bytes), url = VALUES(url), secure_url = VALUES(secure_url), owner_id = VALUES(owner_id), owner_type = VALUES(owner_type), display_name = VALUES(display_name), updated_at = VALUES(updated_at);

INSERT INTO user_stories (id, user_id, media_url, media_type, music_url, music_start, music_end, status, created_at, expired_at) VALUES
('story-seed-linh-01', @linh, 'https://picsum.photos/seed/story-linh/720/1280', 'IMAGE', NULL, NULL, NULL, 'APPROVED', DATE_SUB(@now, INTERVAL 42 MINUTE), DATE_ADD(@now, INTERVAL 21 HOUR)),
('story-seed-quang-01', @quang, 'https://picsum.photos/seed/story-quang/720/1280', 'IMAGE', NULL, NULL, NULL, 'APPROVED', DATE_SUB(@now, INTERVAL 28 MINUTE), DATE_ADD(@now, INTERVAL 22 HOUR)),
('story-seed-mai-01', @mai, 'https://picsum.photos/seed/story-mai/720/1280', 'IMAGE', NULL, NULL, NULL, 'APPROVED', DATE_SUB(@now, INTERVAL 16 MINUTE), DATE_ADD(@now, INTERVAL 23 HOUR)),
('story-seed-bao-01', @bao, 'https://picsum.photos/seed/story-bao/720/1280', 'IMAGE', NULL, NULL, NULL, 'APPROVED', DATE_SUB(@now, INTERVAL 10 MINUTE), DATE_ADD(@now, INTERVAL 20 HOUR))
ON DUPLICATE KEY UPDATE user_id = VALUES(user_id), media_url = VALUES(media_url), media_type = VALUES(media_type), status = VALUES(status), created_at = VALUES(created_at), expired_at = VALUES(expired_at);

INSERT INTO comments (id, post_id, user_id, parent_id, content, comment_type, media_url, timestamp) VALUES
('comment-seed-0003', 'post-seed-quang-01', @viewer, NULL, 'This validates the message history flow from the real API.', 'TEXT', NULL, DATE_SUB(@now, INTERVAL 5 MINUTE)),
('comment-seed-0004', 'post-seed-mai-01', @anna, NULL, 'This belongs in the Friends tab now.', 'TEXT', NULL, DATE_SUB(@now, INTERVAL 9 MINUTE)),
('comment-seed-0005', 'post-seed-bao-01', @linh, NULL, 'Saved and archive screens have enough data to inspect.', 'TEXT', NULL, DATE_SUB(@now, INTERVAL 13 MINUTE))
ON DUPLICATE KEY UPDATE post_id = VALUES(post_id), user_id = VALUES(user_id), content = VALUES(content), comment_type = VALUES(comment_type), timestamp = VALUES(timestamp);

INSERT INTO likes (id, actor_id, target_id, target_type, timestamp) VALUES
('like-seed-0004', @viewer, 'post-seed-quang-01', 'POST', DATE_SUB(@now, INTERVAL 4 MINUTE)),
('like-seed-0005', @anna, 'post-seed-mai-01', 'POST', DATE_SUB(@now, INTERVAL 8 MINUTE)),
('like-seed-0006', @quang, 'post-seed-viewer-01', 'POST', DATE_SUB(@now, INTERVAL 6 MINUTE)),
('like-seed-0007', @mai, 'post-seed-anna-02', 'POST', DATE_SUB(@now, INTERVAL 11 MINUTE)),
('like-seed-0008', @bao, 'post-seed-linh-01', 'POST', DATE_SUB(@now, INTERVAL 16 MINUTE))
ON DUPLICATE KEY UPDATE actor_id = VALUES(actor_id), target_id = VALUES(target_id), target_type = VALUES(target_type), timestamp = VALUES(timestamp);

INSERT INTO conversations (id, conversation_type, title, direct_key, last_message_seq, last_message_id, last_message_at, created_by, created_at, updated_at) VALUES
('conv-seed-0002', 'DIRECT', NULL, REPEAT('d', 64), 3, 'msg-seed-0005', DATE_SUB(@now, INTERVAL 1 MINUTE), @viewer, DATE_SUB(@now, INTERVAL 2 DAY), @now),
('conv-seed-0003', 'DIRECT', NULL, REPEAT('e', 64), 2, 'msg-seed-0007', DATE_SUB(@now, INTERVAL 7 MINUTE), @viewer, DATE_SUB(@now, INTERVAL 2 DAY), @now),
('conv-seed-0004', 'GROUP', 'Pulse builders', NULL, 4, 'msg-seed-0011', DATE_SUB(@now, INTERVAL 3 MINUTE), @viewer, DATE_SUB(@now, INTERVAL 3 DAY), @now)
ON DUPLICATE KEY UPDATE conversation_type = VALUES(conversation_type), title = VALUES(title), last_message_seq = VALUES(last_message_seq), last_message_id = VALUES(last_message_id), last_message_at = VALUES(last_message_at), updated_at = VALUES(updated_at);

INSERT INTO conversation_members (id, conversation_id, user_id, member_role, member_status, joined_seq, last_delivered_seq, last_read_seq, muted_until, joined_at, left_at) VALUES
('conv-mem-seed-03', 'conv-seed-0002', @viewer, 'USER', 'ACTIVE', 1, 3, 3, NULL, DATE_SUB(@now, INTERVAL 2 DAY), NULL),
('conv-mem-seed-04', 'conv-seed-0002', @minh, 'USER', 'ACTIVE', 1, 3, 2, NULL, DATE_SUB(@now, INTERVAL 2 DAY), NULL),
('conv-mem-seed-05', 'conv-seed-0003', @viewer, 'USER', 'ACTIVE', 1, 2, 2, NULL, DATE_SUB(@now, INTERVAL 2 DAY), NULL),
('conv-mem-seed-06', 'conv-seed-0003', @quang, 'USER', 'ACTIVE', 1, 2, 1, NULL, DATE_SUB(@now, INTERVAL 2 DAY), NULL),
('conv-mem-seed-07', 'conv-seed-0004', @viewer, 'ADMIN', 'ACTIVE', 1, 4, 4, NULL, DATE_SUB(@now, INTERVAL 3 DAY), NULL),
('conv-mem-seed-08', 'conv-seed-0004', @anna, 'USER', 'ACTIVE', 1, 4, 4, NULL, DATE_SUB(@now, INTERVAL 3 DAY), NULL),
('conv-mem-seed-09', 'conv-seed-0004', @minh, 'USER', 'ACTIVE', 1, 4, 3, NULL, DATE_SUB(@now, INTERVAL 3 DAY), NULL),
('conv-mem-seed-10', 'conv-seed-0004', @linh, 'USER', 'ACTIVE', 1, 4, 2, NULL, DATE_SUB(@now, INTERVAL 3 DAY), NULL),
('conv-mem-seed-11', 'conv-seed-0004', @mai, 'USER', 'ACTIVE', 1, 4, 2, NULL, DATE_SUB(@now, INTERVAL 3 DAY), NULL)
ON DUPLICATE KEY UPDATE member_role = VALUES(member_role), member_status = VALUES(member_status), last_delivered_seq = VALUES(last_delivered_seq), last_read_seq = VALUES(last_read_seq), left_at = VALUES(left_at);

INSERT INTO messages (id, conversation_id, message_seq, client_message_id, sender_id, message_type, content, metadata, reply_to_seq, created_at, edited_at, deleted_at) VALUES
('msg-seed-0003', 'conv-seed-0002', 1, 'client-seed-0003', @viewer, 'TEXT', 'Minh, can you check the Friends feed after the follow relation update?', NULL, NULL, DATE_SUB(@now, INTERVAL 18 MINUTE), NULL, NULL),
('msg-seed-0004', 'conv-seed-0002', 2, 'client-seed-0004', @minh, 'TEXT', 'Yes, my seeded post appears there now.', NULL, NULL, DATE_SUB(@now, INTERVAL 12 MINUTE), NULL, NULL),
('msg-seed-0005', 'conv-seed-0002', 3, 'client-seed-0005', @viewer, 'TEXT', 'Great. I will verify post detail and comments next.', NULL, NULL, DATE_SUB(@now, INTERVAL 1 MINUTE), NULL, NULL),
('msg-seed-0006', 'conv-seed-0003', 1, 'client-seed-0006', @quang, 'TEXT', 'The compact rail hover feels ready to test.', NULL, NULL, DATE_SUB(@now, INTERVAL 14 MINUTE), NULL, NULL),
('msg-seed-0007', 'conv-seed-0003', 2, 'client-seed-0007', @viewer, 'TEXT', 'I will check mobile overflow after build.', NULL, NULL, DATE_SUB(@now, INTERVAL 7 MINUTE), NULL, NULL),
('msg-seed-0008', 'conv-seed-0004', 1, 'client-seed-0008', @anna, 'TEXT', 'Group thread seeded for chat QA.', NULL, NULL, DATE_SUB(@now, INTERVAL 30 MINUTE), NULL, NULL),
('msg-seed-0009', 'conv-seed-0004', 2, 'client-seed-0009', @linh, 'TEXT', 'Stories and profile cards have more variety now.', NULL, NULL, DATE_SUB(@now, INTERVAL 22 MINUTE), NULL, NULL),
('msg-seed-0010', 'conv-seed-0004', 3, 'client-seed-0010', @mai, 'TEXT', 'Friend suggestions should show several mutual users.', NULL, NULL, DATE_SUB(@now, INTERVAL 11 MINUTE), NULL, NULL),
('msg-seed-0011', 'conv-seed-0004', 4, 'client-seed-0011', @viewer, 'TEXT', 'I will use these rows for direct browser verification.', NULL, NULL, DATE_SUB(@now, INTERVAL 3 MINUTE), NULL, NULL)
ON DUPLICATE KEY UPDATE sender_id = VALUES(sender_id), content = VALUES(content), created_at = VALUES(created_at), edited_at = VALUES(edited_at), deleted_at = VALUES(deleted_at);
INSERT INTO comments (id, post_id, user_id, parent_id, content, comment_type, media_url, timestamp) VALUES
('comment-seed-0006', 'post-seed-ready-01', @viewer, NULL, 'Top feed card comment for detail modal testing.', 'TEXT', NULL, DATE_SUB(@now, INTERVAL 30 SECOND)),
('comment-seed-0007', 'post-seed-ready-02', @anna, NULL, 'Discover card has a visible comment thread now.', 'TEXT', NULL, DATE_SUB(@now, INTERVAL 90 SECOND)),
('comment-seed-0008', 'post-seed-ready-03', @quang, NULL, 'Profile grid post opens with comments too.', 'TEXT', NULL, DATE_SUB(@now, INTERVAL 120 SECOND))
ON DUPLICATE KEY UPDATE post_id = VALUES(post_id), user_id = VALUES(user_id), content = VALUES(content), comment_type = VALUES(comment_type), timestamp = VALUES(timestamp);
INSERT INTO comments (id, post_id, user_id, parent_id, content, comment_type, media_url, timestamp) VALUES
('comment-seed-0009', 'post-seed-linh-01', @viewer, NULL, 'Detail modal comment for the currently ranked Discover post.', 'TEXT', NULL, DATE_SUB(@now, INTERVAL 75 SECOND)),
('comment-seed-0010', 'post-seed-anna-02', @mai, NULL, 'Friends feed item has a comment row for UI testing.', 'TEXT', NULL, DATE_SUB(@now, INTERVAL 85 SECOND)),
('comment-seed-0011', 'post-seed-minh-01', @quang, NULL, 'Search and profile detail can render this comment.', 'TEXT', NULL, DATE_SUB(@now, INTERVAL 95 SECOND))
ON DUPLICATE KEY UPDATE post_id = VALUES(post_id), user_id = VALUES(user_id), content = VALUES(content), comment_type = VALUES(comment_type), timestamp = VALUES(timestamp);

-- Enriched notifications for the responsive notification screen.
INSERT INTO notification_events (id, actor_id, action_type, entity_id, entity_type, created_at) VALUES
('notif-event-seed-04', @anna, 'REPLY_COMMENT', 'comment-seed-0001', 'COMMENT', DATE_SUB(@now, INTERVAL 18 MINUTE)),
('notif-event-seed-05', @minh, 'FOLLOW_BACK', @minh, 'USER', DATE_SUB(@now, INTERVAL 42 MINUTE)),
('notif-event-seed-06', @anna, 'ACCEPT_FRIEND', @anna, 'USER', DATE_SUB(@now, INTERVAL 2 DAY)),
('notif-event-seed-07', @linh, 'MENTION', 'post-seed-linh-01', 'POST', DATE_SUB(@now, INTERVAL 3 DAY)),
('notif-event-seed-08', @minh, 'TAG', 'post-seed-removed-01', 'POST', DATE_SUB(@now, INTERVAL 4 DAY)),
('notif-event-seed-09', @anna, 'STORY_INTERACTION', 'story-seed-viewer-01', 'STORY', DATE_SUB(@now, INTERVAL 5 DAY)),
('notif-event-seed-10', @linh, 'FEATURED_STORY_INTERACTION', 'story-seed-viewer-01', 'STORY', DATE_SUB(@now, INTERVAL 8 DAY)),
('notif-event-seed-11', @minh, 'POST_SHARED', 'post-seed-viewer-01', 'POST', DATE_SUB(@now, INTERVAL 9 DAY)),
('notif-event-seed-12', NULL, 'SECURITY', @viewer, 'USER', DATE_SUB(@now, INTERVAL 11 DAY))
ON DUPLICATE KEY UPDATE actor_id = VALUES(actor_id), action_type = VALUES(action_type), entity_id = VALUES(entity_id), entity_type = VALUES(entity_type), created_at = VALUES(created_at);

INSERT INTO user_notifications (id, user_id, event_id, notification_status, read_at, created_at) VALUES
('user-notif-seed-04', @viewer, 'notif-event-seed-04', 'UNREAD', NULL, DATE_SUB(@now, INTERVAL 18 MINUTE)),
('user-notif-seed-05', @viewer, 'notif-event-seed-05', 'UNREAD', NULL, DATE_SUB(@now, INTERVAL 42 MINUTE)),
('user-notif-seed-06', @viewer, 'notif-event-seed-06', 'READ', DATE_SUB(@now, INTERVAL 1 DAY), DATE_SUB(@now, INTERVAL 2 DAY)),
('user-notif-seed-07', @viewer, 'notif-event-seed-07', 'UNREAD', NULL, DATE_SUB(@now, INTERVAL 3 DAY)),
('user-notif-seed-08', @viewer, 'notif-event-seed-08', 'READ', DATE_SUB(@now, INTERVAL 3 DAY), DATE_SUB(@now, INTERVAL 4 DAY)),
('user-notif-seed-09', @viewer, 'notif-event-seed-09', 'READ', DATE_SUB(@now, INTERVAL 4 DAY), DATE_SUB(@now, INTERVAL 5 DAY)),
('user-notif-seed-10', @viewer, 'notif-event-seed-10', 'READ', DATE_SUB(@now, INTERVAL 7 DAY), DATE_SUB(@now, INTERVAL 8 DAY)),
('user-notif-seed-11', @viewer, 'notif-event-seed-11', 'UNREAD', NULL, DATE_SUB(@now, INTERVAL 9 DAY)),
('user-notif-seed-12', @viewer, 'notif-event-seed-12', 'UNREAD', NULL, DATE_SUB(@now, INTERVAL 11 DAY))
ON DUPLICATE KEY UPDATE user_id = VALUES(user_id), event_id = VALUES(event_id), notification_status = VALUES(notification_status), read_at = VALUES(read_at), created_at = VALUES(created_at);
INSERT INTO post_reposts (id, actor_id, post_id, post_owner_id, created_at) VALUES
('repost-seed-0001', @viewer, 'post-seed-anna-01', @anna, DATE_SUB(@now, INTERVAL 7 MINUTE)),
('repost-seed-0002', @viewer, 'post-seed-minh-01', @minh, DATE_SUB(@now, INTERVAL 20 MINUTE)),
('repost-seed-0003', @anna, 'post-seed-viewer-01', @viewer, DATE_SUB(@now, INTERVAL 11 MINUTE)),
('repost-seed-0004', @quang, 'post-seed-mai-01', @mai, DATE_SUB(@now, INTERVAL 9 MINUTE))
ON DUPLICATE KEY UPDATE actor_id = VALUES(actor_id), post_id = VALUES(post_id), post_owner_id = VALUES(post_owner_id), created_at = VALUES(created_at);
-- profile-enrichment-20260724
INSERT INTO user_job (id, user_id, company_name, position, from_date, to_date, is_public) VALUES
('job-seed-viewer-01', @viewer, 'Pulse Studio', 'Frontend integration engineer', '2024-01-01', NULL, TRUE),
('job-seed-anna-01', @anna, 'Form Studio', 'Product designer', '2023-01-01', NULL, TRUE),
('job-seed-minh-01', @minh, 'Street Archive', 'Street photographer', '2022-01-01', NULL, TRUE),
('job-seed-linh-01', @linh, 'Paper House', 'Illustrator', '2023-06-01', NULL, TRUE),
('job-seed-quang-01', @quang, 'FPT Software', 'Backend engineer', '2022-08-01', NULL, TRUE),
('job-seed-mai-01', @mai, 'Open Road', 'Travel creator', '2023-03-01', NULL, TRUE),
('job-seed-bao-01', @bao, 'Market Notes', 'Product analyst', '2024-02-01', NULL, TRUE)
ON DUPLICATE KEY UPDATE company_name = VALUES(company_name), position = VALUES(position), from_date = VALUES(from_date), to_date = VALUES(to_date), is_public = VALUES(is_public);

INSERT INTO user_university (id, user_id, school_name, major, from_date, to_date, is_graduate, is_public) VALUES
('university-seed-viewer-01', @viewer, 'Da Nang University of Technology', NULL, NULL, NULL, FALSE, TRUE),
('university-seed-anna-01', @anna, 'University of Architecture Ho Chi Minh City', NULL, NULL, NULL, FALSE, TRUE),
('university-seed-minh-01', @minh, 'Hue University', NULL, NULL, NULL, FALSE, TRUE),
('university-seed-linh-01', @linh, 'Can Tho University', NULL, NULL, NULL, FALSE, TRUE),
('university-seed-quang-01', @quang, 'FPT University', NULL, NULL, NULL, FALSE, TRUE),
('university-seed-mai-01', @mai, 'Nha Trang University', NULL, NULL, NULL, FALSE, TRUE),
('university-seed-bao-01', @bao, 'University of Economics', NULL, NULL, NULL, FALSE, TRUE)
ON DUPLICATE KEY UPDATE school_name = VALUES(school_name), major = VALUES(major), from_date = VALUES(from_date), to_date = VALUES(to_date), is_graduate = VALUES(is_graduate), is_public = VALUES(is_public);

INSERT INTO user_social_media (id, user_id, link) VALUES
('social-seed-viewer-01', @viewer, 'https://github.com/codex-fe-test'),
('social-seed-viewer-02', @viewer, 'https://linkedin.com/in/codex-fe-test'),
('social-seed-anna-01', @anna, 'https://dribbble.com/anna-nguyen'),
('social-seed-minh-01', @minh, 'https://instagram.com/minh-street-photo'),
('social-seed-mai-01', @mai, 'https://youtube.com/@mai-travel')
ON DUPLICATE KEY UPDATE user_id = VALUES(user_id), link = VALUES(link);

INSERT INTO musics (id, slug_name, display_name, descriptions, display_images, single_name, song_url, duration, category, release_date) VALUES
('1364979', 'quiet-city-lines', 'Quiet City Lines', 'Seeded music for image stories and post items.', 'https://picsum.photos/seed/music-1364979/360/360', 'Seed Audio', 'https://cdn.example.local/music/1364979.mp3', 184, 'seed', CURDATE()),
('1368743', 'morning-archive', 'Morning Archive', 'Seeded music for image stories and post items.', 'https://picsum.photos/seed/music-1368743/360/360', 'Seed Audio', 'https://cdn.example.local/music/1368743.mp3', 202, 'seed', CURDATE()),
('1380625', 'soft-window-light', 'Soft Window Light', 'Seeded music for image stories and post items.', 'https://picsum.photos/seed/music-1380625/360/360', 'Seed Audio', 'https://cdn.example.local/music/1380625.mp3', 176, 'seed', CURDATE()),
('1380632', 'late-feed-motion', 'Late Feed Motion', 'Seeded music for image stories and post items.', 'https://picsum.photos/seed/music-1380632/360/360', 'Seed Audio', 'https://cdn.example.local/music/1380632.mp3', 193, 'seed', CURDATE()),
('1381324', 'minimal-street-loop', 'Minimal Street Loop', 'Seeded music for image stories and post items.', 'https://picsum.photos/seed/music-1381324/360/360', 'Seed Audio', 'https://cdn.example.local/music/1381324.mp3', 210, 'seed', CURDATE()),
('1384752', 'gray-coastline', 'Gray Coastline', 'Seeded music for image stories and post items.', 'https://picsum.photos/seed/music-1384752/360/360', 'Seed Audio', 'https://cdn.example.local/music/1384752.mp3', 167, 'seed', CURDATE()),
('1407062', 'studio-pulse', 'Studio Pulse', 'Seeded music for image stories and post items.', 'https://picsum.photos/seed/music-1407062/360/360', 'Seed Audio', 'https://cdn.example.local/music/1407062.mp3', 188, 'seed', CURDATE()),
('1421761', 'clean-carousel', 'Clean Carousel', 'Seeded music for image stories and post items.', 'https://picsum.photos/seed/music-1421761/360/360', 'Seed Audio', 'https://cdn.example.local/music/1421761.mp3', 199, 'seed', CURDATE()),
('1449007', 'story-afterglow', 'Story Afterglow', 'Seeded music for image stories and post items.', 'https://picsum.photos/seed/music-1449007/360/360', 'Seed Audio', 'https://cdn.example.local/music/1449007.mp3', 205, 'seed', CURDATE()),
('1451827', 'monochrome-daybook', 'Monochrome Daybook', 'Seeded music for image stories and post items.', 'https://picsum.photos/seed/music-1451827/360/360', 'Seed Audio', 'https://cdn.example.local/music/1451827.mp3', 181, 'seed', CURDATE())
ON DUPLICATE KEY UPDATE display_name = VALUES(display_name), song_url = VALUES(song_url), duration = VALUES(duration), category = VALUES(category);

UPDATE user_stories SET music_id = '1364979' WHERE id IN ('story-seed-viewer-01', 'story-seed-anna-01');
UPDATE user_stories SET music_id = '1368743' WHERE id IN ('story-seed-minh-01', 'story-seed-linh-01');
UPDATE user_stories SET music_id = '1380625' WHERE id IN ('story-seed-quang-01', 'story-seed-mai-01');

INSERT INTO user_stories (id, user_id, media_url, media_type, music_id, music_url, music_start, music_end, status, created_at, expired_at) VALUES
('story-seed-video-anna-01', @anna, 'https://interactive-examples.mdn.mozilla.net/media/cc0-videos/flower.mp4', 'VIDEO', NULL, NULL, NULL, NULL, 'APPROVED', DATE_SUB(@now, INTERVAL 7 MINUTE), DATE_ADD(@now, INTERVAL 20 HOUR)),
('story-seed-image-music-mai-02', @mai, 'https://picsum.photos/seed/story-mai-music/720/1280', 'IMAGE', '1384752', NULL, 12, 32, 'APPROVED', DATE_SUB(@now, INTERVAL 11 MINUTE), DATE_ADD(@now, INTERVAL 21 HOUR)),
('story-seed-video-quang-01', @quang, 'https://interactive-examples.mdn.mozilla.net/media/cc0-videos/flower.webm', 'VIDEO', NULL, NULL, NULL, NULL, 'APPROVED', DATE_SUB(@now, INTERVAL 14 MINUTE), DATE_ADD(@now, INTERVAL 22 HOUR))
ON DUPLICATE KEY UPDATE user_id = VALUES(user_id), media_url = VALUES(media_url), media_type = VALUES(media_type), music_id = VALUES(music_id), music_url = VALUES(music_url), music_start = VALUES(music_start), music_end = VALUES(music_end), status = VALUES(status), created_at = VALUES(created_at), expired_at = VALUES(expired_at);

INSERT INTO post_details (post_id, user_id, content, hashtag, created_at, updated_at, validate_status, music_id, music_start, music_end) VALUES
('post-seed-video-anna-03', @anna, 'Short video post for carousel and detail testing. This video keeps item music empty by rule.', '["video","story","carousel"]', DATE_SUB(@now, INTERVAL 4 MINUTE), DATE_SUB(@now, INTERVAL 4 MINUTE), 'APPROVED', NULL, NULL, NULL),
('post-seed-image-music-mai-03', @mai, 'Image carousel with per-item captions and image-level music metadata.', '["music","image","postitems"]', DATE_SUB(@now, INTERVAL 8 MINUTE), DATE_SUB(@now, INTERVAL 8 MINUTE), 'APPROVED', NULL, NULL, NULL)
ON DUPLICATE KEY UPDATE user_id = VALUES(user_id), content = VALUES(content), hashtag = VALUES(hashtag), created_at = VALUES(created_at), updated_at = VALUES(updated_at), validate_status = VALUES(validate_status), music_id = VALUES(music_id), music_start = VALUES(music_start), music_end = VALUES(music_end);

INSERT INTO media (asset_id, public_id, width, height, media_format, resource_type, bytes, url, secure_url, owner_id, owner_type, version, version_id, display_name, created_at, updated_at) VALUES
('media-post-video-anna-03', 'seed/video/anna-03', 1280, 720, 'mp4', 'video', 990000, 'https://interactive-examples.mdn.mozilla.net/media/cc0-videos/flower.mp4', 'https://interactive-examples.mdn.mozilla.net/media/cc0-videos/flower.mp4', 'post-seed-video-anna-03', 'POST', '1', 'seed', 'Seed video post', DATE_SUB(@now, INTERVAL 4 MINUTE), DATE_SUB(@now, INTERVAL 4 MINUTE)),
('media-post-image-mai-03-a', 'seed/post/mai-03-a', 1200, 1500, 'jpg', 'image', 390000, 'https://picsum.photos/seed/mai-post-music-a/1200/1500', 'https://picsum.photos/seed/mai-post-music-a/1200/1500', 'post-seed-image-music-mai-03', 'POST', '1', 'seed', 'Mai image music A', DATE_SUB(@now, INTERVAL 8 MINUTE), DATE_SUB(@now, INTERVAL 8 MINUTE)),
('media-post-image-mai-03-b', 'seed/post/mai-03-b', 1200, 1500, 'jpg', 'image', 390000, 'https://picsum.photos/seed/mai-post-music-b/1200/1500', 'https://picsum.photos/seed/mai-post-music-b/1200/1500', 'post-seed-image-music-mai-03', 'POST', '1', 'seed', 'Mai image music B', DATE_SUB(@now, INTERVAL 7 MINUTE), DATE_SUB(@now, INTERVAL 7 MINUTE))
ON DUPLICATE KEY UPDATE public_id = VALUES(public_id), width = VALUES(width), height = VALUES(height), media_format = VALUES(media_format), resource_type = VALUES(resource_type), bytes = VALUES(bytes), url = VALUES(url), secure_url = VALUES(secure_url), owner_id = VALUES(owner_id), owner_type = VALUES(owner_type), display_name = VALUES(display_name), updated_at = VALUES(updated_at);

INSERT INTO post_items (id, post_id, order_number, media_id, caption, music_id, music_start, music_end, created_at, updated_at) VALUES
('post-item-seed-video-anna-03-01', 'post-seed-video-anna-03', 1, 'media-post-video-anna-03', 'Video item caption. Music fields are empty for videos.', NULL, NULL, NULL, DATE_SUB(@now, INTERVAL 4 MINUTE), DATE_SUB(@now, INTERVAL 4 MINUTE)),
('post-item-seed-mai-03-01', 'post-seed-image-music-mai-03', 1, 'media-post-image-mai-03-a', 'First image has its own caption and music segment.', '1421761', 8, 28, DATE_SUB(@now, INTERVAL 8 MINUTE), DATE_SUB(@now, INTERVAL 8 MINUTE)),
('post-item-seed-mai-03-02', 'post-seed-image-music-mai-03', 2, 'media-post-image-mai-03-b', 'Second image has a different caption and music segment.', '1451827', 0, 20, DATE_SUB(@now, INTERVAL 7 MINUTE), DATE_SUB(@now, INTERVAL 7 MINUTE))
ON DUPLICATE KEY UPDATE media_id = VALUES(media_id), caption = VALUES(caption), music_id = VALUES(music_id), music_start = VALUES(music_start), music_end = VALUES(music_end), updated_at = VALUES(updated_at);