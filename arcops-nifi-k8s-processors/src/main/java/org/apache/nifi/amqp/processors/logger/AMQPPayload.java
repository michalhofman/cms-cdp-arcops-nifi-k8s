package org.apache.nifi.amqp.processors.logger;

import lombok.Data;

import java.util.Map;
import java.util.UUID;

@Data
class AMQPPayload {

    private final byte[] body;
    private final Map<String, String> attributes;

    String getMessageId() {
        final String messageId = attributes.get("amqp$messageId");
        return messageId != null ? messageId : generateId();
    }

    private String generateId() {
        return UUID.randomUUID().toString() + "-internal";
    }
}
