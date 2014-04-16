package org.graylog2.inputs.mqtt;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import net.sf.xenqtt.client.AsyncMqttClient;
import net.sf.xenqtt.client.MqttClientConfig;
import net.sf.xenqtt.client.Subscription;
import net.sf.xenqtt.message.ConnectReturnCode;
import net.sf.xenqtt.message.QoS;
import org.graylog2.plugin.buffers.Buffer;
import org.graylog2.plugin.configuration.ConfigurationException;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.NumberField;
import org.graylog2.plugin.configuration.fields.TextField;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.inputs.MisfireException;
import org.graylog2.plugin.system.NodeId;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class MQTTInput extends MessageInput {
    private final org.slf4j.Logger LOG = LoggerFactory.getLogger(MQTTInput.class);
    private final MetricRegistry metricRegistry;
    private final NodeId nodeId;
    private AsyncMqttClient client;
    private List<String> topics;

    @Inject
    public MQTTInput(MetricRegistry metricRegistry,
                     NodeId nodeId) {
        super();
        this.metricRegistry = metricRegistry;
        this.nodeId = nodeId;
    }

    @Override
    public void checkConfiguration() throws ConfigurationException {
        try {
            if (configuration.getString("brokerUrl") == null || configuration.getString("brokerUrl").isEmpty()
                || configuration.getString("topic") == null || configuration.getString("topic").isEmpty()
                || configuration.getInt("keepalive") > 0 || configuration.getInt("timeout") > 0 || configuration.getInt("threads") > 0)
                throw new ConfigurationException("Invalid or incomplete input configuration specified!");
        } catch (NullPointerException e) {
            throw new ConfigurationException("Invalid input configuration specified!");
        }
    }

    @Override
    public void launch(Buffer processBuffer) throws MisfireException {
        String brokerUrl = configuration.getString("brokerUrl");
        String topic = configuration.getString("topic");
        topics = Lists.newArrayList();
        topics.add(topic);

        final int threadPoolSize = (int)configuration.getInt("threads");
        final AsyncMQTTClientListener listener = new AsyncMQTTClientListener(processBuffer, this, topics, metricRegistry);
        this.client = new AsyncMqttClient(brokerUrl, listener, threadPoolSize, getMqttClientConfig());
        final List<Subscription> subscriptions = Lists.newArrayList();

        final AtomicReference<ConnectReturnCode> connectReturnCode = new AtomicReference<ConnectReturnCode>();

        try {
            client.connect("graylog2-"+nodeId.toString(), true);
            ConnectReturnCode returnCode = connectReturnCode.get();
            if (returnCode == null || returnCode != ConnectReturnCode.ACCEPTED) {
                LOG.error("Unable to connect to the MQTT broker. Reason: " + returnCode);
                return;
            }

            for (String thistopic : topics)
                subscriptions.add(new Subscription(thistopic, QoS.EXACTLY_ONCE));

            client.subscribe(subscriptions);
        } catch (Exception ex) {
            LOG.error("An unexpected exception has occurred.", ex);
        }

    }

    @Override
    public void stop() {
        client.unsubscribe(topics);
        client.disconnect();
    }

    @Override
    public ConfigurationRequest getRequestedConfiguration() {
        ConfigurationRequest cr = new ConfigurationRequest();
        cr.addField(new TextField("brokerUrl",
                "Broker URL",
                "tcp://localhost:8083",
                "This is the URL of the MQTT broker.",
                ConfigurationField.Optional.NOT_OPTIONAL));

        cr.addField(new TextField("topic",
                "Topic Name",
                "cluster/system/logs",
                "The topic you are subscribing to.",
                ConfigurationField.Optional.NOT_OPTIONAL));

        cr.addField(new NumberField("threads",
                "Thread pool size",
                5,
                "Number of threads to use for message processing"));

        cr.addField(new NumberField("timeout",
                "Connection timeout",
                30,
                "Amount of seconds to wait for connections"));

        cr.addField(new NumberField("keepalive",
                "Keepalive interval",
                300,
                "Maximum amount of seconds to wait before sending keepalive message"));

        return cr;
    }

    @Override
    public boolean isExclusive() {
        return false;
    }

    @Override
    public String getName() {
        return "MQTT Input";
    }

    @Override
    public String linkToDocs() {
        return "http://www.graylog2.org";
    }

    @Override
    public Map<String, Object> getAttributes() {
        return configuration.getSource();
    }

    private MqttClientConfig getMqttClientConfig() {
        MqttClientConfig clientConfig = new MqttClientConfig();
        clientConfig.setConnectTimeoutSeconds((int)configuration.getInt("timeout"));
        clientConfig.setKeepAliveSeconds((int)configuration.getInt("keepalive"));

        return clientConfig;
    }
}