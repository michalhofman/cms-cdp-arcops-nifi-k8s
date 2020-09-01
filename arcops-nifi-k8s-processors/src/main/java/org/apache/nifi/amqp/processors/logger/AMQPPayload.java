package org.apache.nifi.amqp.processors.logger;

import lombok.Data;

import java.util.Map;

@Data
class AMQPPayload {

    private final byte[] body;
    private final Map<String, String> attributes;

    String getMessageId() {
        return attributes.get("amqp$messageId");
    }
}
