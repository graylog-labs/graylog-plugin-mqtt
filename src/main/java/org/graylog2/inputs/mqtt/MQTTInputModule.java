package org.graylog2.inputs.mqtt;

import org.graylog2.plugin.PluginModule;

public class MQTTInputModule extends PluginModule {
    @Override
    protected void configure() {
        addTransport("mqtt-transport", MQTTTransport.class, MQTTTransport.Config.class, MQTTTransport.Factory.class);

        addMessageInput(MQTTGELFInput.class, MQTTGELFInput.Factory.class);
        addMessageInput(MQTTRawInput.class, MQTTRawInput.Factory.class);
        addMessageInput(MQTTSyslogInput.class, MQTTSyslogInput.Factory.class);
    }
}
