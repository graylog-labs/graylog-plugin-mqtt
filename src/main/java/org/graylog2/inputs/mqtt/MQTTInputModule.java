package org.graylog2.inputs.mqtt;

import org.graylog2.plugin.PluginModule;

/**
 * @author Dennis Oelkers <dennis@torch.sh>
 */
public class MQTTInputModule extends PluginModule {
    @Override
    protected void configure() {
        addMessageInput(MQTTInput.class);
    }
}
