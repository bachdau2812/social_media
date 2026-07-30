package com.dauducbach.clone.modules.post.service;

public record SseFanoutMessage(String userId, String event, String data) {
}