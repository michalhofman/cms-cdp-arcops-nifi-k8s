package org.apache.nifi.amqp.processors.logger;

import com.google.gson.Gson;
import org.apache.nifi.logging.ComponentLog;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;


@NotThreadSafe
class DefaultDatabaseLogger implements DatabaseLogger {

    private static final Gson gson = new Gson();

    private final PreparedStatement statement;
    private final ComponentLog logger;

    DefaultDatabaseLogger(Connection dbConnection, String query, ComponentLog logger) throws SQLException {
        this.logger = logger;
        this.statement = dbConnection.prepareStatement(query);
    }

    public void save(AMQPPayload payload) throws SQLException {
        // statement.setString(DEF_PARAM_MESSAGEID, payload.getMessageId());
        // statement.setBlob(DEF_PARAM_PAYLOAD, new ByteArrayInputStream(payload.getBody()));
        // statement.setString(DEF_PARAM_ATTRIBUTES, gson.toJson(payload.getAttributes()));

        statement.setString(1, payload.getMessageId());
        statement.setBlob(2, new ByteArrayInputStream(payload.getBody()));
        statement.setString(3, gson.toJson(payload.getAttributes()));


        logger.debug("Executing statement: {}", new Object[]{statement.getParameterMetaData().toString()});

        statement.execute();
        logger.debug("Statement executed");
    }

    @Override
    public void close() throws Exception {
        statement.close();
    }
}
