package org.graylog2.inputs.mqtt;

import org.graylog2.plugin.PluginModule;

public class MQTTInputModule extends PluginModule {
    @Override
    protected void configure() {
        installTransport(transportMapBinder(), "mqtt-transport", MQTTTransport.class);
        installInput(inputsMapBinder(), MQTTGELFInput.class, MQTTGELFInput.Factory.class);
    }
}
