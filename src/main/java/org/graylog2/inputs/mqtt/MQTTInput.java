package org.graylog2.inputs.mqtt;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import net.sf.xenqtt.client.AsyncMqttClient;
import net.sf.xenqtt.client.MqttClient;
import net.sf.xenqtt.client.MqttClientConfig;
import net.sf.xenqtt.client.Subscription;
import net.sf.xenqtt.message.ConnectReturnCode;
import net.sf.xenqtt.message.QoS;
import org.graylog2.plugin.buffers.Buffer;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationException;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.NumberField;
import org.graylog2.plugin.configuration.fields.TextField;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.inputs.MisfireException;
import org.graylog2.plugin.system.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

public class MQTTInput extends MessageInput {
    private static final String CK_BROKER_URL = "brokerUrl";
    private static final String CK_THREADS = "threads";
    private static final String CK_TOPICS = "topics";
    private static final String CK_TIMEOUT = "timeout";
    private static final String CK_KEEPALIVE = "keepalive";
    private static final String CK_PASSWORD = "password";
    private static final String CK_USERNAME = "username";
    private static final String CK_USE_AUTH = "useAuth";

    private final Logger LOG = LoggerFactory.getLogger(MQTTInput.class);

    private final MetricRegistry metricRegistry;
    private final NodeId nodeId;

    private MqttClient client;
    private List<String> topics;

    @Inject
    public MQTTInput(final MetricRegistry metricRegistry,
                     final NodeId nodeId) {
        this(metricRegistry, nodeId, null, null);
    }

    @VisibleForTesting
    MQTTInput(final MetricRegistry metricRegistry, final NodeId nodeId,
              final MqttClient client, final List<String> topics) {
        this.metricRegistry = metricRegistry;
        this.nodeId = nodeId;
        this.client = client;
        this.topics = topics;
    }

    @Override
    public void checkConfiguration(final Configuration configuration) throws ConfigurationException {
        if (!configuration.stringIsSet(CK_BROKER_URL)) {
            throw new ConfigurationException("MQTT broker URL must not be empty.");
        }

        if (!configuration.stringIsSet(CK_TOPICS)) {
            throw new ConfigurationException("MQTT topics must not be empty.");
        }

        if (!configuration.intIsSet(CK_KEEPALIVE) || configuration.getInt(CK_KEEPALIVE) < 0) {
            throw new ConfigurationException("Keep-alive interval must be a positive number.");
        }

        if (!configuration.intIsSet(CK_TIMEOUT) || configuration.getInt(CK_TIMEOUT) < 0) {
            throw new ConfigurationException("Connection timeout must be a positive number.");
        }

        if (!configuration.intIsSet(CK_THREADS) || configuration.getInt(CK_THREADS) < 0) {
            throw new ConfigurationException("Thread pool size must be a positive number.");
        }

        if (configuration.getBoolean(CK_USE_AUTH) &&
                (!configuration.stringIsSet(CK_USERNAME) || !configuration.stringIsSet(CK_PASSWORD))) {
            throw new ConfigurationException("Username and password are required for authentication.");
        }
    }

    @Override
    public void launch(final Buffer processBuffer) throws MisfireException {
        if (topics == null) {
            topics = buildTopicList();
        }

        if (client == null) {
            client = buildClient(processBuffer);
        }

        final String clientId = "graylog2_" + Hashing.murmur3_32().hashUnencodedChars(nodeId.toString()).toString();
        try {
            final ConnectReturnCode returnCode;
            if (configuration.getBoolean(CK_USE_AUTH)) {
                final String username = configuration.getString(CK_USERNAME);
                final String password = configuration.getString(CK_PASSWORD);
                returnCode = client.connect(clientId, true, username, password);
            } else {
                returnCode = client.connect(clientId, true);
            }

            if (returnCode != null) {
                LOG.error("Unable to connect to the MQTT broker. Reason: " + returnCode);
                return;
            }

            client.subscribe(buildSubscriptions());
        } catch (Exception ex) {
            LOG.error("An unexpected exception has occurred.", ex);
        }
    }

    private List<Subscription> buildSubscriptions() {
        final ImmutableList.Builder<Subscription> subscriptions = ImmutableList.builder();
        for (String topic : topics) {
            subscriptions.add(new Subscription(topic, QoS.EXACTLY_ONCE));
        }

        return subscriptions.build();
    }

    private MqttClient buildClient(Buffer processBuffer) {
        final String brokerUrl = configuration.getString(CK_BROKER_URL);
        final int threadPoolSize = (int) configuration.getInt(CK_THREADS);
        final AsyncMQTTClientListener listener = new AsyncMQTTClientListener(processBuffer, this, metricRegistry);

        return new AsyncMqttClient(brokerUrl, listener, threadPoolSize, buildClientConfiguration());
    }

    private List<String> buildTopicList() {
        final Iterable<String> topicIterable = Splitter.on(',')
                .omitEmptyStrings()
                .trimResults()
                .split(configuration.getString(CK_TOPICS));
        return ImmutableList.copyOf(topicIterable);
    }

    @Override
    public void stop() {
        client.unsubscribe(topics);
        client.disconnect();
    }

    @Override
    public ConfigurationRequest getRequestedConfiguration() {
        final ConfigurationRequest cr = new ConfigurationRequest();

        cr.addField(new TextField(CK_BROKER_URL,
                "Broker URL",
                "tcp://localhost:1883",
                "This is the URL of the MQTT broker."));

        cr.addField(new TextField(CK_USE_AUTH,
                "Username",
                "",
                "This is the username for connecting to the MQTT broker.",
                ConfigurationField.Optional.OPTIONAL));

        cr.addField(new TextField(CK_USERNAME,
                "Username",
                "",
                "This is the username for connecting to the MQTT broker.",
                ConfigurationField.Optional.OPTIONAL));

        cr.addField(new TextField(CK_PASSWORD,
                "Password",
                "",
                "This is the password for connecting to the MQTT broker.",
                ConfigurationField.Optional.OPTIONAL,
                TextField.Attribute.IS_PASSWORD));

        cr.addField(new TextField(CK_TOPICS,
                "Topic Names",
                "cluster/system/logs",
                "The comma-separated list of topics you are subscribing to."));

        cr.addField(new NumberField(CK_THREADS,
                "Thread pool size",
                5,
                "Number of threads to use for message processing"));

        cr.addField(new NumberField(CK_TIMEOUT,
                "Connection timeout",
                30,
                "Amount of seconds to wait for connections"));

        cr.addField(new NumberField(CK_KEEPALIVE,
                "Keep-alive interval",
                300,
                "Maximum amount of seconds to wait before sending keep-alive message"));

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
        return Maps.transformEntries(getConfiguration().getSource(), new Maps.EntryTransformer<String, Object, Object>() {
            @Override
            public Object transformEntry(String key, Object value) {
                if (CK_PASSWORD.equals(key)) {
                    return "****";
                }
                return value;
            }
        });
    }

    private MqttClientConfig buildClientConfiguration() {
        return new MqttClientConfig()
                .setConnectTimeoutSeconds((int) configuration.getInt(CK_TIMEOUT))
                .setKeepAliveSeconds((int) configuration.getInt(CK_KEEPALIVE));
    }
}