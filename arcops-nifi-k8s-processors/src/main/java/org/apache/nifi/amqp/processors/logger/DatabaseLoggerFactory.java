package org.apache.nifi.amqp.processors.logger;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;

import java.sql.Connection;
import java.sql.SQLException;

public class DatabaseLoggerFactory {

    private final static DatabaseLogger NOP_INSTANCE = new NopDatabaseLogger();

    private final Connection connection;
    private final ComponentLog logger;

    public DatabaseLoggerFactory(Connection connection, ComponentLog logger) {
        this.connection = connection;
        this.logger = logger;
    }

    public DatabaseLogger getDatabaseLogger(String queryTemplate) {
        if (connection != null) {
            try {
                logger.info("Using DefaultDatabaseLogger as DB connection is available");
                return new DefaultDatabaseLogger(connection, queryTemplate, logger);
            } catch (SQLException ex) {
                throw new ProcessException("Failed to prepare statement", ex);
            }
        }
        logger.info("Using NopDatabaseLogger as DB connection is not available");
        return NOP_INSTANCE;
    }
}
