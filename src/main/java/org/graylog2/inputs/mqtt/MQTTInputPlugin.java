package org.graylog2.inputs.mqtt;

import com.google.common.collect.Lists;
import org.graylog2.plugin.Plugin;
import org.graylog2.plugin.PluginModule;

import java.util.Collection;
import java.util.List;

/**
 * @author Dennis Oelkers <dennis@torch.sh>
 */
public class MQTTInputPlugin implements Plugin {
    public Collection<PluginModule> modules() {
        List<PluginModule> result = Lists.newArrayList();
        result.add(new MQTTInputModule());
        return result;
    }
}
