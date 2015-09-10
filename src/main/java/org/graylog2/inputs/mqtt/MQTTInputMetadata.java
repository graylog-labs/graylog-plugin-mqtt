package org.graylog2.inputs.mqtt;

import org.graylog2.plugin.PluginMetaData;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.Version;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public class MQTTInputMetadata implements PluginMetaData {
    @Override
    public String getUniqueId() {
        return MQTTGELFInput.class.getCanonicalName();
    }

    @Override
    public String getName() {
        return "MQTT Input Plugin";
    }

    @Override
    public String getAuthor() {
        return "TORCH GmbH";
    }

    @Override
    public URI getURL() {
        return URI.create("http://www.torch.sh");
    }

    @Override
    public Version getVersion() {
        return new Version(1, 1, 1);
    }

    @Override
    public String getDescription() {
        return "Process messages from one or multiple topics of an MQTT broker.";
    }

    @Override
    public Version getRequiredVersion() {
        return new Version(1, 0, 0);
    }

    @Override
    public Set<ServerStatus.Capability> getRequiredCapabilities() {
        return new HashSet<>();
    }
}
