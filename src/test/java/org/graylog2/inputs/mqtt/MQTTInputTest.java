package org.graylog2.inputs.mqtt;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import net.sf.xenqtt.client.MqttClient;
import net.sf.xenqtt.client.Subscription;
import net.sf.xenqtt.message.QoS;
import org.graylog2.plugin.buffers.Buffer;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationException;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.inputs.MisfireException;
import org.graylog2.plugin.system.NodeId;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MQTTInputTest {
    private static final String[] VALID_CONFIG_KEYS = new String[]{
            "brokerUrl",
            "threads",
            "topics",
            "timeout",
            "keepalive",
            "password",
            "username",
            "useAuth"
    };
    private static final ImmutableMap<String, Object> VALID_CONFIG_MAP = ImmutableMap.<String, Object>builder()
            .put("brokerUrl", "tcp://example.com:1234")
            .put("useAuth", true)
            .put("username", "TEST-username")
            .put("password", "TEST-password")
            .put("topics", "test1,test2")
            .put("threads", 5)
            .put("timeout", 1000)
            .put("keepalive", 1000)
            .build();

    private final NodeId nodeId;
    private MQTTInput mqttInput;
    private static final Configuration VALID_CONFIGURATION = new Configuration(VALID_CONFIG_MAP);

    public MQTTInputTest() throws IOException {
        final File nodeIdFile = File.createTempFile("MQTTInputTest", ".tmp");
        nodeIdFile.deleteOnExit();
        nodeId = new NodeId(nodeIdFile.getAbsolutePath());
    }

    @Before
    public void setUp() throws IOException {
        mqttInput = new MQTTInput(new MetricRegistry(), nodeId);
    }

    @Test(expected = ConfigurationException.class)
    public void checkConfigurationFailsIfBrokerUrlIsMissing() throws ConfigurationException {
        mqttInput.checkConfiguration(validConfigurationWithout("brokerUrl"));
    }

    @Test(expected = ConfigurationException.class)
    public void checkConfigurationFailsIfTopicsAreMissing() throws ConfigurationException {
        mqttInput.checkConfiguration(validConfigurationWithout("topics"));
    }

    @Test(expected = ConfigurationException.class)
    public void checkConfigurationFailsIfThreadPoolSizeIsMissing() throws ConfigurationException {
        mqttInput.checkConfiguration(validConfigurationWithout("threads"));
    }

    @Test(expected = ConfigurationException.class)
    public void checkConfigurationFailsIfConnectionTimeoutIsMissing() throws ConfigurationException {
        mqttInput.checkConfiguration(validConfigurationWithout("timeout"));
    }

    @Test(expected = ConfigurationException.class)
    public void checkConfigurationFailsIfKeepAliveIntervalIsMissing() throws ConfigurationException {
        mqttInput.checkConfiguration(validConfigurationWithout("keepalive"));
    }

    @Test(expected = ConfigurationException.class)
    public void checkConfigurationFailsIfAuthRequiredAndUsernameIsMissing() throws ConfigurationException {
        mqttInput.checkConfiguration(validConfigurationWithout("username"));
    }

    @Test(expected = ConfigurationException.class)
    public void checkConfigurationFailsIfAuthRequiredAndPasswordIsMissing() throws ConfigurationException {
        mqttInput.checkConfiguration(validConfigurationWithout("password"));
    }

    @Test
    public void checkConfigurationSucceedsWithValidConfiguration() throws ConfigurationException {
        mqttInput.checkConfiguration(VALID_CONFIGURATION);
    }

    @Test
    public void checkConfigurationSucceedsWithoutUsernameAndPassword() throws ConfigurationException {
        final Configuration configuration = new Configuration(ImmutableMap.<String, Object>builder()
                .put("brokerUrl", "tcp://example.com:1234")
                .put("useAuth", false)
                .put("topics", "test1,test2")
                .put("threads", 5)
                .put("timeout", 1000)
                .put("keepalive", 1000)
                .build());

        mqttInput.checkConfiguration(configuration);
    }

    @Test
    public void testLaunch() throws MisfireException {
        final ImmutableMap<String, Object> configMap = ImmutableMap.<String, Object>builder()
                            .put("brokerUrl", "tcp://iot.eclipse.org:1883")
                            .put("useAuth", false)
                            .put("topics", "graylog2-mqtt-input-" + this.getClass().getSimpleName())
                            .put("threads", 5)
                            .put("timeout", 1000)
                            .put("keepalive", 1000)
                            .build();
        final Configuration configuration = new Configuration(configMap);
        final Buffer processBuffer = mock(Buffer.class);
        final MQTTInput input = new MQTTInput(new MetricRegistry(), nodeId);

        input.initialize(configuration);
        input.setConfiguration(configuration);
        input.launch(processBuffer);
    }

    @Test
    public void testStop() {
        final MqttClient client = mock(MqttClient.class);
        final List<String> topics = ImmutableList.of("test");
        final MQTTInput input = new MQTTInput(new MetricRegistry(), nodeId, client, topics);

        input.stop();

        verify(client, times(1)).unsubscribe(topics);
        verify(client, times(1)).disconnect();
    }

    @Test
    public void testGetRequestedConfiguration() {
        final ConfigurationRequest configurationRequest = mqttInput.getRequestedConfiguration();

        assertThat(configurationRequest.asList().keySet(), hasItems(VALID_CONFIG_KEYS));
    }

    @Test
    public void testIsExclusive() {
        assertThat(mqttInput.isExclusive(), is(false));
    }

    @Test
    public void testGetName() {
        assertThat(mqttInput.getName(), equalTo("MQTT Input"));
    }

    @Test
    public void testLinkToDocs() {
        assertThat(mqttInput.linkToDocs(), equalTo("http://www.graylog2.org"));
    }

    @Test
    public void testGetAttributes() throws ConfigurationException {
        mqttInput.initialize(VALID_CONFIGURATION);
        mqttInput.setConfiguration(VALID_CONFIGURATION);

        final Map<String, Object> attributes = mqttInput.getAttributes();
        assertThat(attributes.keySet(), hasItems(VALID_CONFIG_KEYS));
        assertThat((String) attributes.get("password"), equalTo("****"));
    }

    private Configuration validConfigurationWithout(final String key) {
        return new Configuration(Maps.filterEntries(VALID_CONFIG_MAP, new Predicate<Map.Entry<String, Object>>() {
            @Override
            public boolean apply(Map.Entry<String, Object> input) {
                return key.equals(input.getKey());
            }
        }));
    }
}