package com.dauducbach.clone.modules.auth.infrastructure.kafka;

import com.dauducbach.clone.modules.auth.notifications.AuthNotificationPublisher;
import com.dauducbach.clone.modules.auth.registration.RegistrationDraft;
import com.dauducbach.clone.commons.serialization.GsonUtils;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

@Component
@RequiredArgsConstructor
public class KafkaAuthNotificationPublisher implements AuthNotificationPublisher {
    private final KafkaSender<String, String> kafkaSender;

    @Override
    public Mono<Void> sendRegistrationCode(String email, String username, String code) {
        JsonObject payload = new JsonObject();
        payload.addProperty("username", username);
        payload.addProperty("email", email);
        payload.addProperty("code", code);
        return send("auth_send_code", email, payload, "Send verify code for new registration user");
    }

    @Override
    public Mono<Void> publishProfileCreated(RegistrationDraft draft, String userId) {
        JsonObject payload = GsonUtils.fromObject(draft);
        payload.remove("password");
        payload.addProperty("userId", userId);
        return send("profile_creation_event", userId, payload, "User Creation Event");
    }

    @Override
    public Mono<Void> sendRecoveryCode(String email, String code) {
        JsonObject payload = new JsonObject();
        payload.addProperty("code", code);
        payload.addProperty("email", email);
        return send("forget_password_event", email, payload,
                "Gui code de nguoi dung xac nhan email de doi mat khau");
    }

    @Override
    public Mono<Void> sendNewPassword(String email, String newPassword) {
        JsonObject payload = new JsonObject();
        payload.addProperty("email", email);
        payload.addProperty("newPassword", newPassword);
        return send("new_password_event", email, payload, "Send new password to User");
    }

    @Override
    public Mono<Void> sendNewUsernameAndPassword(String email, String newPassword) {
        JsonObject payload = new JsonObject();
        payload.addProperty("email", email);
        payload.addProperty("newPassword", newPassword);
        return send("new_password_and_username_event", email, payload, "Send new password to User");
    }

    private Mono<Void> send(String topic, String key, JsonObject payload, String correlationId) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, payload.toString());
        SenderRecord<String, String, String> senderRecord = SenderRecord.create(record, correlationId);
        return kafkaSender.send(Mono.just(senderRecord)).then();
    }
}
