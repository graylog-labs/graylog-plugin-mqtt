package org.graylog2.inputs.mqtt;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableSet;
import org.graylog2.plugin.Plugin;
import org.graylog2.plugin.PluginMetaData;
import org.graylog2.plugin.PluginModule;

import java.util.Collection;

@AutoService(Plugin.class)
public class MQTTInputPlugin implements Plugin {
    @Override
    public PluginMetaData metadata() {
        return new MQTTInputMetadata();
    }

    public Collection<PluginModule> modules() {
        return ImmutableSet.<PluginModule>of(new MQTTInputModule());
    }
}
