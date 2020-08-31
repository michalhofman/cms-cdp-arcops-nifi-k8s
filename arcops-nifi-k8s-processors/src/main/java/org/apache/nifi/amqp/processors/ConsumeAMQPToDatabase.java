package org.apache.nifi.amqp.processors;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;
import org.apache.nifi.amqp.processors.logger.DatabaseLogger;
import org.apache.nifi.amqp.processors.logger.DatabaseLoggerFactory;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.ValidationResult.Builder;
import org.apache.nifi.components.Validator;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.nifi.amqp.processors.logger.DatabaseLogger.DEF_PARAM_ATTRIBUTES;
import static org.apache.nifi.amqp.processors.logger.DatabaseLogger.DEF_PARAM_MESSAGEID;
import static org.apache.nifi.amqp.processors.logger.DatabaseLogger.DEF_PARAM_PAYLOAD;


@Tags({"amqp", "rabbit", "get", "message", "receive", "consume", "log"})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Extended version of original NiFi ConsumeAMQP processors that addtionaly exposes in attributes the RoutingKey if it's available in the message. Consumes AMQP Messages from an AMQP Broker using the AMQP 0.9.1 protocol. Each message that is received from the AMQP Broker will be emitted as its own FlowFile to the 'success' relationship.")
@WritesAttributes({@WritesAttribute(
    attribute = "amqp$appId",
    description = "The App ID field from the AMQP Message"
), @WritesAttribute(
    attribute = "amqp$contentEncoding",
    description = "The Content Encoding reported by the AMQP Message"
), @WritesAttribute(
    attribute = "amqp$contentType",
    description = "The Content Type reported by the AMQP Message"
), @WritesAttribute(
    attribute = "amqp$headers",
    description = "The headers present on the AMQP Message"
), @WritesAttribute(
    attribute = "amqp$deliveryMode",
    description = "The numeric indicator for the Message's Delivery Mode"
), @WritesAttribute(
    attribute = "amqp$priority",
    description = "The Message priority"
), @WritesAttribute(
    attribute = "amqp$correlationId",
    description = "The Message's Correlation ID"
), @WritesAttribute(
    attribute = "amqp$replyTo",
    description = "The value of the Message's Reply-To field"
), @WritesAttribute(
    attribute = "amqp$expiration",
    description = "The Message Expiration"
), @WritesAttribute(
    attribute = "amqp$messageId",
    description = "The unique ID of the Message"
), @WritesAttribute(
    attribute = "amqp$routingKey",
    description = "RoutingKey used to route message from Exchange to the queue"
), @WritesAttribute(
    attribute = "amqp$timestamp",
    description = "The timestamp of the Message, as the number of milliseconds since epoch"
), @WritesAttribute(
    attribute = "amqp$type",
    description = "The type of message"
), @WritesAttribute(
    attribute = "amqp$userId",
    description = "The ID of the user"
), @WritesAttribute(
    attribute = "amqp$clusterId",
    description = "The ID of the AMQP Cluster"
)})
public class ConsumeAMQPToDatabase extends ExtendedConsumeAMQP {

    static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
        .name("Database Controller Service")
        .description("The Controller Service to obtain connection to database")
        .required(false)
        .identifiesControllerService(DBCPService.class)
        .build();

    static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
        .name("Query")
        .description("SQL query for storing message payload and attributes in database. Required if 'Database Controller Service' is configured. \n"
            + "Query is expected to provide ? placeholders for following parameters (in that order): `messageId` (string), `payload` (blob), `attributes` (JSON as blob). \n"
            + "They can be ignored in database, but are expected to be present in the query executed by this processor. "
            + "Query can also contain constants - anything you can pass to jdbc driver as prepared statement" )
        .required(false)
        .addValidator(getQueryValidator())
        .build();

    private ComponentLog log;
    private java.sql.Connection dbConnection = null;
    private DatabaseLogger dbLogger;

    @OnScheduled
    @SuppressWarnings("unused")
    public void onScheduled(ProcessContext context) {
        log = this.getLogger();
        if (context.getProperty(DBCP_SERVICE).isSet()) {
            log.debug("OnScheduled: setting DB connection");
            dbConnection = getConnection(context);
            log.info("OnScheduled: DB connection set");
        }
        dbLogger = new DatabaseLoggerFactory(dbConnection, log).getDatabaseLogger(context.getProperty(QUERY).getValue());
    }

    private java.sql.Connection getConnection(ProcessContext context) {
        return context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class).getConnection();
    }

    @OnStopped
    @SuppressWarnings("unused")
    public void onStopped() throws Exception {
        if (dbConnection != null && !dbConnection.isClosed()) {
            log.debug("OnStopped: closing DB connection");

            if (!dbConnection.getAutoCommit()) {
                dbConnection.commit();
            }

            dbLogger.close();
            dbConnection.close();
            log.info("OnStopped: DB connection closed");
        }
    }

    @Override
    protected void processResource(Connection amqpConnection, AMQPConsumer consumer, ProcessContext context, ProcessSession session) {
        GetResponse lastReceived = null;

        try {
            for (int i = 0; i < context.getProperty(BATCH_SIZE).asInteger(); ++i) {
                GetResponse response = consumer.consume();
                if (response == null) {
                    if (lastReceived == null) {
                        context.yield();
                    }
                    break;
                }

                FlowFile flowFile = session.create();
                flowFile = session.write(flowFile, (out) -> out.write(response.getBody()));
                BasicProperties amqpProperties = response.getProps();

                Map<String, String> attributes = this.buildAttributes(amqpProperties);
                attributes.put("amqp$routingKey", response.getEnvelope().getRoutingKey() != null
                    ? response.getEnvelope().getRoutingKey() : UUID.randomUUID().toString());

                dbLogger.save(response.getBody(), attributes);

                flowFile = session.putAllAttributes(flowFile, attributes);
                session.getProvenanceReporter().receive(flowFile, amqpConnection.toString() + "/" + context.getProperty(QUEUE).getValue());
                session.transfer(flowFile, REL_SUCCESS);
                lastReceived = response;
            }

            session.commit();

            if (lastReceived != null) {
                try {
                    consumer.acknowledge(lastReceived);
                } catch (IOException ex) {
                    throw new ProcessException("Failed to acknowledge message", ex);
                }
            }
        } catch (SQLException ex) {
            throw new ProcessException("Failed SQL operation ", ex);
        } catch (ProcessException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ProcessException("Failed operation ", ex);
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> supportedPropertyDescriptors = new ArrayList<>(super.getSupportedPropertyDescriptors());
        supportedPropertyDescriptors.add(DBCP_SERVICE);
        supportedPropertyDescriptors.add(QUERY);
        return Collections.unmodifiableList(supportedPropertyDescriptors);
    }

    private static Validator getQueryValidator() {
        return new Validator() {
            @Override
            public ValidationResult validate(String subject, String input, ValidationContext context) {
                return new Builder()
                    .input(input)
                    .subject(subject)
                    .valid((context.getProperty(DBCP_SERVICE).isSet() && containsNamedParameters(input)) || !context.getProperty(DBCP_SERVICE).isSet())
                    .explanation(String.format("Query need to specify all required named parameters: %s, %s, %s",DEF_PARAM_MESSAGEID, DEF_PARAM_PAYLOAD, DEF_PARAM_ATTRIBUTES) )
                    .build();
            }

            private boolean containsNamedParameters(String query) {
                return query != null;
                    // && query.contains(DEF_PARAM_ATTRIBUTES)
                    // && query.contains(DEF_PARAM_MESSAGEID)
                    // && query.contains(DEF_PARAM_PAYLOAD);
            }
        };
    }
}
