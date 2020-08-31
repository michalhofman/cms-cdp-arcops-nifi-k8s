package org.apache.nifi.amqp.processors.logger;

import java.sql.SQLException;
import java.util.Map;

public interface DatabaseLogger extends AutoCloseable {

    String DEF_PARAM_ATTRIBUTES = "attributes";
    String DEF_PARAM_PAYLOAD = "payload";
    String DEF_PARAM_MESSAGEID = "messageId";

    default void save(byte[] payload, Map<String, String> attributes) throws SQLException {
        save(new AMQPPayload(payload, attributes));
    }

    void save(AMQPPayload payload) throws SQLException;
}
