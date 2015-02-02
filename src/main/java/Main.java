import net.sf.xenqtt.client.AsyncClientListener;
import net.sf.xenqtt.client.AsyncMqttClient;
import net.sf.xenqtt.client.MqttClient;
import net.sf.xenqtt.client.PublishMessage;
import net.sf.xenqtt.client.Subscription;
import net.sf.xenqtt.message.ConnectReturnCode;
import net.sf.xenqtt.message.QoS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Produces hit music from days gone by.
 */
public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String... args) throws Throwable {
        final CountDownLatch connectLatch = new CountDownLatch(1);
        final AtomicReference<ConnectReturnCode> connectReturnCode = new AtomicReference<ConnectReturnCode>();
        AsyncClientListener listener = new AsyncClientListener() {

            @Override
            public void publishReceived(MqttClient client, PublishMessage message) {
                log.warn("Received a message when no subscriptions were active. Check your broker ;)");
            }

            @Override
            public void disconnected(MqttClient client, Throwable cause, boolean reconnecting) {
                if (cause != null) {
                    log.error("Disconnected from the broker due to an exception.", cause);
                } else {
                    log.info("Disconnected from the broker.");
                }

                if (reconnecting) {
                    log.info("Attempting to reconnect to the broker.");
                }
            }

            @Override
            public void connected(MqttClient client, ConnectReturnCode returnCode) {
                connectReturnCode.set(returnCode);
                connectLatch.countDown();
            }

            @Override
            public void subscribed(MqttClient client, Subscription[] requestedSubscriptions, Subscription[] grantedSubscriptions, boolean requestsGranted) {
            }

            @Override
            public void unsubscribed(MqttClient client, String[] topics) {
            }

            @Override
            public void published(MqttClient client, PublishMessage message) {
            }

        };

        // Build your client. This client is an asynchronous one so all interaction with the broker will be non-blocking.
        MqttClient client = new AsyncMqttClient("tcp://localhost:1883", listener, 5);
        try {
            // Connect to the broker. We will await the return code so that we know whether or not we can even begin publishing.
            client.connect("musicProducerAsync", false, "music-user", "music-pass");
            connectLatch.await();

            ConnectReturnCode returnCode = connectReturnCode.get();
            if (returnCode == null || returnCode != ConnectReturnCode.ACCEPTED) {
                // The broker bounced us. We are done.
                log.error("The broker rejected our attempt to connect. Reason: " + returnCode);
                return;
            }

            // Publish a musical catalog
            client.publish(new PublishMessage("cluster/system/logs", QoS.AT_MOST_ONCE, "{\"version\":\"1.1\",\"host\":\"example.org\",\"short_message\":\"A short message that helps you identify what is going on\",\"full_message\":\"Backtrace here\\n\\nmore stuff\",\"timestamp\":1397484488.3072,\"level\":1,\"_user_id\":9001,\"_some_info\":\"foo\",\"_some_env_var\":\"bar\"}"));
        } catch (Exception ex) {
            log.error("An exception prevented the publishing of the full catalog.", ex);
        } finally {
            // We are done. Disconnect.
            if (!client.isClosed()) {
                client.disconnect();
            }
        }
    }

}