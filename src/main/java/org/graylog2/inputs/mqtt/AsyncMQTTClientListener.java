package org.graylog2.inputs.mqtt;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import net.sf.xenqtt.client.*;
import net.sf.xenqtt.message.ConnectReturnCode;
import net.sf.xenqtt.message.QoS;
import org.graylog2.inputs.gelf.gelf.GELFParser;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.buffers.Buffer;
import org.graylog2.plugin.buffers.BufferOutOfCapacityException;
import org.graylog2.plugin.buffers.ProcessingDisabledException;
import org.graylog2.plugin.inputs.MessageInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * @author Dennis Oelkers <dennis@torch.sh>
 */
public class AsyncMQTTClientListener implements AsyncClientListener {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncMQTTClientListener.class);

    private final Buffer processBuffer;
    private final MessageInput messageInput;
    private final List<String> topics;
    private final GELFParser gelfParser;

    private final Meter incomingMessages;
    private final Meter incompleteMessages;
    private final Meter processedMessages;

    public AsyncMQTTClientListener(Buffer processBuffer,
                                   MessageInput messageInput,
                                   List<String> topics,
                                   MetricRegistry metricRegistry) {
        this.processBuffer = processBuffer;
        this.messageInput = messageInput;
        this.topics = topics;
        this.gelfParser = new GELFParser(metricRegistry);

        final String metricName = messageInput.getUniqueReadableId();

        this.incomingMessages = metricRegistry.meter(name(metricName, "incomingMessages"));
        this.incompleteMessages = metricRegistry.meter(name(metricName, "incompleteMessages"));
        this.processedMessages = metricRegistry.meter(name(metricName, "processedMessages"));
    }

    @Override
    public void connected(MqttClient mqttClient, ConnectReturnCode connectReturnCode) {
        LOG.info("Connected to MQTT broker ...");
        List<Subscription> subscriptions = Lists.newArrayList();
        for (String topic : topics)
            subscriptions.add(new Subscription(topic, QoS.EXACTLY_ONCE));
        mqttClient.subscribe(subscriptions);
    }

    @Override
    public void subscribed(MqttClient mqttClient, Subscription[] subscriptions, Subscription[] subscriptions2, boolean b) {
        LOG.info("Subscribed to topics: " + subscriptions.toString());
    }

    @Override
    public void unsubscribed(MqttClient mqttClient, String[] strings) {
    }

    @Override
    public void published(MqttClient mqttClient, PublishMessage publishMessage) {
    }

    @Override
    public void publishReceived(MqttClient mqttClient, PublishMessage publishMessage) {
        LOG.debug("Received message: " + publishMessage.getPayloadString());
        incomingMessages.mark();

        if (publishMessage.getPayloadString() == null) {
            LOG.debug("Received message is null. Not processing.");
            return;
        }

        if (publishMessage.isDuplicate() || publishMessage.isEmpty()) {
            LOG.debug("Received message is a duplicate or empty. Not processing.");
            incompleteMessages.mark();
            return;
        }

        Message message = gelfParser.parse(publishMessage.getPayloadString(), messageInput);
        if (message == null) {
            LOG.error("Unable to parse message received: ", publishMessage.getPayloadString());
            incompleteMessages.mark();
            return;
        }

        try {
            LOG.debug("Parsed message successfully, message id: <{}>. Enqueuing into process buffer.", message.getId());
            message.addField("_mqtt_topic", publishMessage.getTopic());
            message.addField("_mqtt_received_timestamp", publishMessage.getReceivedTimestamp());
            processBuffer.insertFailFast(message, messageInput);
            publishMessage.ack();
            processedMessages.mark();
        } catch (BufferOutOfCapacityException | ProcessingDisabledException e) {
            LOG.error("Unable to insert message into process buffer: ", e);
        }
    }

    @Override
    public void disconnected(MqttClient mqttClient, Throwable throwable, boolean b) {
    }
}
