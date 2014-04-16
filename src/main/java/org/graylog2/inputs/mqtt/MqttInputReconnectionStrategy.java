package org.graylog2.inputs.mqtt;

import net.sf.xenqtt.client.MqttClient;
import net.sf.xenqtt.client.ReconnectionStrategy;
import org.joda.time.DateTime;

/**
 * @author Dennis Oelkers <dennis@torch.sh>
 */
public class MqttInputReconnectionStrategy implements ReconnectionStrategy {
    private DateTime lastConnect;

    public MqttInputReconnectionStrategy() {
    }

    @Override
    public long connectionLost(MqttClient client, Throwable cause) {
        return 0;
    }

    @Override
    public void connectionEstablished() {
        lastConnect = DateTime.now();
    }

    @Override
    public ReconnectionStrategy clone() {
        return new MqttInputReconnectionStrategy();
    }
}
