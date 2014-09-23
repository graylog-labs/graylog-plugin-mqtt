package org.graylog2.inputs.mqtt;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import net.sf.xenqtt.client.AsyncClientListener;
import net.sf.xenqtt.client.MqttClient;
import net.sf.xenqtt.client.PublishMessage;
import net.sf.xenqtt.client.Subscription;
import net.sf.xenqtt.message.ConnectReturnCode;
import org.graylog2.inputs.gelf.gelf.GELFParser;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.buffers.Buffer;
import org.graylog2.plugin.buffers.BufferOutOfCapacityException;
import org.graylog2.plugin.buffers.ProcessingDisabledException;
import org.graylog2.plugin.inputs.MessageInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static com.codahale.metrics.MetricRegistry.name;

public class AsyncMQTTClientListener implements AsyncClientListener {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncMQTTClientListener.class);

    private final Buffer processBuffer;
    private final MessageInput messageInput;
    private final GELFParser gelfParser;

    private final Meter incomingMessages;
    private final Meter incompleteMessages;
    private final Meter processedMessages;

    public AsyncMQTTClientListener(final Buffer processBuffer,
                                   final MessageInput messageInput,
                                   final MetricRegistry metricRegistry) {
        this.processBuffer = processBuffer;
        this.messageInput = messageInput;
        this.gelfParser = new GELFParser(metricRegistry);

        final String metricName = messageInput.getUniqueReadableId();

        this.incomingMessages = metricRegistry.meter(name(metricName, "incomingMessages"));
        this.incompleteMessages = metricRegistry.meter(name(metricName, "incompleteMessages"));
        this.processedMessages = metricRegistry.meter(name(metricName, "processedMessages"));
    }

    @Override
    public void connected(MqttClient mqttClient, ConnectReturnCode connectReturnCode) {
        if (connectReturnCode == ConnectReturnCode.ACCEPTED) {
            LOG.info("Connected MQTT client");
        }
    }

    @Override
    public void subscribed(final MqttClient mqttClient,
                           final Subscription[] requestedSubscriptions,
                           final Subscription[] grantedSubscriptions,
                           final boolean requestsGranted) {
        if (!requestsGranted) {
            LOG.warn("Couldn't subscribe to all requested topics: {}", Arrays.toString(requestedSubscriptions));
        }

        LOG.info("Subscribed to topics: {}", Arrays.toString(grantedSubscriptions));
    }

    @Override
    public void unsubscribed(MqttClient mqttClient, String[] topics) {
        LOG.info("Unsubscribed from topics: {}", Arrays.toString(topics));
    }

    @Override
    public void published(MqttClient mqttClient, PublishMessage message) {
        LOG.trace("Published message {}", message);
    }

    @Override
    public void publishReceived(MqttClient mqttClient, PublishMessage message) {
        LOG.debug("Received message: {}", message);
        incomingMessages.mark();

        if (message.isEmpty()) {
            LOG.debug("Received message is empty. Not processing.");
            incompleteMessages.mark();
            return;
        }

        if (message.isDuplicate()) {
            LOG.debug("Received message is a duplicate. Not processing.");
            incompleteMessages.mark();
            return;
        }

        final Message gelfMessage;
        try {
            gelfMessage = gelfParser.parse(message.getPayloadString(), messageInput);
        } catch (Exception e) {
            LOG.warn("Unable to parse received message: {}", message);
            incompleteMessages.mark();
            return;
        }

        try {
            LOG.debug("Parsed message successfully, message id: <{}>. Inserting into process buffer.", gelfMessage.getId());
            gelfMessage.addField("_mqtt_topic", message.getTopic());
            gelfMessage.addField("_mqtt_received_timestamp", message.getReceivedTimestamp());
            processBuffer.insertFailFast(gelfMessage, messageInput);
            message.ack();
            processedMessages.mark();
        } catch (BufferOutOfCapacityException | ProcessingDisabledException e) {
            LOG.error("Unable to insert message into process buffer: ", e);
            incompleteMessages.mark();
        }
    }

    @Override
    public void disconnected(final MqttClient client, final Throwable cause, final boolean reconnecting) {
        LOG.info("Disconnected MQTT client", cause);
    }
}
