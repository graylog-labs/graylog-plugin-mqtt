package org.graylog2.inputs.mqtt;

import com.google.common.collect.ImmutableSet;
import org.graylog2.plugin.Plugin;
import org.graylog2.plugin.PluginModule;

import java.util.Collection;

public class MQTTInputPlugin implements Plugin {
    public Collection<PluginModule> modules() {
        return ImmutableSet.<PluginModule>of(new MQTTInputModule());
    }
}
