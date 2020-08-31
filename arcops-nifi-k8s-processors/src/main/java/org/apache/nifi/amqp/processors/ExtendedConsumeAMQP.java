package org.apache.nifi.amqp.processors;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Tags({"amqp", "rabbit", "get", "message", "receive", "consume"})
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
public class ExtendedConsumeAMQP extends ConsumeAMQP {

    @Override
    protected void processResource(Connection connection, AMQPConsumer consumer, ProcessContext context, ProcessSession session) {
        GetResponse lastReceived = null;

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
            attributes.put("amqp$routingKey", response.getEnvelope().getRoutingKey());
            flowFile = session.putAllAttributes(flowFile, attributes);
            session.getProvenanceReporter().receive(flowFile, connection.toString() + "/" + context.getProperty(QUEUE).getValue());
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

    }

    Map<String, String> buildAttributes(BasicProperties properties) {
        Map<String, String> attributes = new HashMap<>();
        this.addAttribute(attributes, "amqp$appId", properties.getAppId());
        this.addAttribute(attributes, "amqp$contentEncoding", properties.getContentEncoding());
        this.addAttribute(attributes, "amqp$contentType", properties.getContentType());
        this.addAttribute(attributes, "amqp$headers", properties.getHeaders());
        this.addAttribute(attributes, "amqp$deliveryMode", properties.getDeliveryMode());
        this.addAttribute(attributes, "amqp$priority", properties.getPriority());
        this.addAttribute(attributes, "amqp$correlationId", properties.getCorrelationId());
        this.addAttribute(attributes, "amqp$replyTo", properties.getReplyTo());
        this.addAttribute(attributes, "amqp$expiration", properties.getExpiration());
        this.addAttribute(attributes, "amqp$messageId", properties.getMessageId());
        this.addAttribute(attributes, "amqp$timestamp", properties.getTimestamp() == null ? null : properties.getTimestamp().getTime());
        this.addAttribute(attributes, "amqp$type", properties.getType());
        this.addAttribute(attributes, "amqp$userId", properties.getUserId());
        this.addAttribute(attributes, "amqp$clusterId", properties.getClusterId());
        return attributes;
    }

    void addAttribute(Map<String, String> attributes, String attributeName, Object value) {
        if (value != null) {
            attributes.put(attributeName, value.toString());
        }
    }

}
