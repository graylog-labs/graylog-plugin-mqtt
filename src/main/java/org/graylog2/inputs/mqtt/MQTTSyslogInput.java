package org.graylog2.inputs.mqtt;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import org.graylog2.inputs.codecs.SyslogCodec;
import org.graylog2.plugin.LocalMetricRegistry;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.inputs.MessageInput;

import javax.inject.Inject;

public class MQTTSyslogInput extends MessageInput {
    private static final String NAME = "MQTT TCP (Syslog)";

    @AssistedInject
    public MQTTSyslogInput(final MetricRegistry metricRegistry,
                           @Assisted Configuration configuration,
                           MQTTTransport.Factory mqttTransportFactory,
                           SyslogCodec.Factory syslogCodecFactory,
                           LocalMetricRegistry localRegistry,
                           Config config,
                           Descriptor descriptor, ServerStatus serverStatus) {
        super(metricRegistry, configuration, mqttTransportFactory.create(configuration), localRegistry, syslogCodecFactory.create(configuration), config, descriptor, serverStatus);
    }

    public interface Factory extends MessageInput.Factory<MQTTSyslogInput> {
        @Override
        MQTTSyslogInput create(Configuration configuration);

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
        public Config(MQTTTransport.Factory transport, SyslogCodec.Factory codec) {
            super(transport.getConfig(), codec.getConfig());
        }
    }
}