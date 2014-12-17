package org.graylog2.inputs.mqtt;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import net.sf.xenqtt.client.AsyncMqttClient;
import net.sf.xenqtt.client.MqttClient;
import net.sf.xenqtt.client.MqttClientConfig;
import net.sf.xenqtt.client.Subscription;
import net.sf.xenqtt.message.ConnectReturnCode;
import net.sf.xenqtt.message.QoS;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.buffers.Buffer;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.BooleanField;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.NumberField;
import org.graylog2.plugin.configuration.fields.TextField;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.inputs.MisfireException;
import org.graylog2.plugin.inputs.annotations.ConfigClass;
import org.graylog2.plugin.inputs.annotations.FactoryClass;
import org.graylog2.plugin.inputs.codecs.CodecAggregator;
import org.graylog2.plugin.inputs.transports.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MQTTTransport implements Transport {
    private final Logger LOG = LoggerFactory.getLogger(MQTTTransport.class);

    private static final String CK_BROKER_URL = "brokerUrl";
    private static final String CK_THREADS = "threads";
    private static final String CK_TOPICS = "topics";
    private static final String CK_TIMEOUT = "timeout";
    private static final String CK_KEEPALIVE = "keepalive";
    private static final String CK_PASSWORD = "password";
    private static final String CK_USERNAME = "username";
    private static final String CK_USE_AUTH = "useAuth";

    private final Configuration configuration;
    private final MetricRegistry metricRegistry;
    private ServerStatus serverStatus;
    private MqttClient client;
    private List<String> topics;


    @AssistedInject
    public MQTTTransport(@Assisted Configuration configuration,
                         MetricRegistry metricRegistry,
                         ServerStatus serverStatus) {
        this.configuration = configuration;
        this.metricRegistry = metricRegistry;
        this.serverStatus = serverStatus;
    }

    @Override
    public void setMessageAggregator(CodecAggregator codecAggregator) {

    }

    @Override
    public void launch(MessageInput messageInput) throws MisfireException {
        if (topics == null) {
            topics = buildTopicList();
        }

        if (client == null) {
            client = buildClient(messageInput);
        }

        final String clientId = "graylog2_" + Hashing.murmur3_32().hashUnencodedChars(this.serverStatus.getNodeId().toString()).toString();
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
            }
        } catch (Exception ex) {
            LOG.error("An unexpected exception has occurred.", ex);
        }
    }

    private List<Subscription> buildSubscriptions() {
        final ImmutableList.Builder<Subscription> subscriptions = ImmutableList.builder();
        for (String topic : topics) {
            subscriptions.add(new Subscription(topic, QoS.AT_LEAST_ONCE));
        }

        return subscriptions.build();
    }

    private MqttClient buildClient(MessageInput messageInput) {
        final String brokerUrl = configuration.getString(CK_BROKER_URL);
        final int threadPoolSize = (int) configuration.getInt(CK_THREADS);
        final AsyncMQTTClientListener listener = new AsyncMQTTClientListener(messageInput, buildSubscriptions(), metricRegistry);

        return new AsyncMqttClient(brokerUrl, listener, threadPoolSize, buildClientConfiguration());
    }

    private List<String> buildTopicList() {
        final Iterable<String> topicIterable = Splitter.on(',')
                .omitEmptyStrings()
                .trimResults()
                .split(configuration.getString(CK_TOPICS));
        return ImmutableList.copyOf(topicIterable);
    }

    private MqttClientConfig buildClientConfiguration() {
        return new MqttClientConfig()
                .setConnectTimeoutSeconds((int) configuration.getInt(CK_TIMEOUT))
                .setKeepAliveSeconds((int) configuration.getInt(CK_KEEPALIVE));
    }

    @Override
    public void stop() {
        client.unsubscribe(topics);
        client.disconnect();
    }


    @Override
    public MetricSet getMetricSet() {
        return null;
    }

    @FactoryClass
    public interface Factory extends Transport.Factory<MQTTTransport> {
        @Override
        MQTTTransport create(Configuration configuration);

        @Override
        Transport.Config getConfig();
    }

    @ConfigClass
    public static class Config implements Transport.Config {
        @Override
        public ConfigurationRequest getRequestedConfiguration() {
            final ConfigurationRequest cr = new ConfigurationRequest();

            cr.addField(new TextField(CK_BROKER_URL,
                    "Broker URL",
                    "tcp://localhost:1883",
                    "This is the URL of the MQTT broker."));

            cr.addField(new BooleanField(CK_USE_AUTH,
                    "Use Authentication",
                    false,
                    "This is the username for connecting to the MQTT broker."));

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
    }
}
