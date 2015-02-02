package org.graylog2.inputs.mqtt;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import net.sf.xenqtt.client.AsyncClientListener;
import net.sf.xenqtt.client.MqttClient;
import net.sf.xenqtt.client.PublishMessage;
import net.sf.xenqtt.client.Subscription;
import net.sf.xenqtt.message.ConnectReturnCode;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.journal.RawMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static com.codahale.metrics.MetricRegistry.name;

public class AsyncMQTTClientListener implements AsyncClientListener {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncMQTTClientListener.class);

    private final MessageInput messageInput;
    private final List<Subscription> subscriptions;

    private final Meter incomingMessages;
    private final Meter incompleteMessages;
    private final Meter processedMessages;

    public AsyncMQTTClientListener(final MessageInput messageInput,
                                   final List<Subscription> subscriptions,
                                   final MetricRegistry metricRegistry) {
        this.messageInput = messageInput;
        this.subscriptions = subscriptions;

        final String metricName = messageInput.getUniqueReadableId();

        this.incomingMessages = metricRegistry.meter(name(metricName, "incomingMessages"));
        this.incompleteMessages = metricRegistry.meter(name(metricName, "incompleteMessages"));
        this.processedMessages = metricRegistry.meter(name(metricName, "processedMessages"));
    }

    @Override
    public void connected(MqttClient client, ConnectReturnCode returnCode) {
        if (returnCode == ConnectReturnCode.ACCEPTED) {
            LOG.info("Connected MQTT client");

            client.subscribe(subscriptions);
        } else {
            LOG.error("MQTT client not connected! Reason: {}", returnCode);
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

        final RawMessage rawMessage = new RawMessage(message.getPayload());

        LOG.debug("Parsed message successfully, message id: <{}>.", rawMessage.getId());
        messageInput.processRawMessage(rawMessage);
        message.ack();
        processedMessages.mark();
    }

    @Override
    public void disconnected(final MqttClient client, final Throwable cause, final boolean reconnecting) {
        LOG.info("Disconnected MQTT client", cause);
    }
}
