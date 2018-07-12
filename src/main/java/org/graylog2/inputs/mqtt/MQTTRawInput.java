package org.graylog2.inputs.mqtt;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import org.graylog2.inputs.codecs.RawCodec;
import org.graylog2.plugin.LocalMetricRegistry;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.inputs.MessageInput;

import javax.inject.Inject;

public class MQTTRawInput extends MessageInput {
    private static final String NAME = "MQTT TCP (Raw/Plaintext)";

    @AssistedInject
    public MQTTRawInput(final MetricRegistry metricRegistry,
                        @Assisted Configuration configuration,
                        MQTTTransport.Factory mqttTransportFactory,
                        RawCodec.Factory rawCodecFactory,
                        LocalMetricRegistry localRegistry,
                        Config config,
                        Descriptor descriptor, ServerStatus serverStatus) {
        super(metricRegistry, configuration, mqttTransportFactory.create(configuration), localRegistry, rawCodecFactory.create(configuration), config, descriptor, serverStatus);
    }

    public interface Factory extends MessageInput.Factory<MQTTRawInput> {
        @Override
        MQTTRawInput create(Configuration configuration);

        @Override
        Config getConfig();

        @Override
        Descriptor getDescriptor();
    }

    public static class Descriptor extends MessageInput.Descriptor {
        @Inject
        public Descriptor() {
            super(NAME, false, "https://github.com/graylog-labs/graylog-plugin-mqtt");
        }
    }

    public static class Config extends MessageInput.Config {
        @Inject
        public Config(MQTTTransport.Factory transport, RawCodec.Factory codec) {
            super(transport.getConfig(), codec.getConfig());
        }
    }
}