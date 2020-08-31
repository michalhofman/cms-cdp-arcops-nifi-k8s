package org.apache.nifi.amqp.processors.logger;

class NopDatabaseLogger implements DatabaseLogger {

    @Override
    public void save(AMQPPayload payload) {
    }

    @Override
    public void close() {
    }
}
