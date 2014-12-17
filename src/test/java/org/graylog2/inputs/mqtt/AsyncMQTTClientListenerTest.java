package org.graylog2.inputs.mqtt;

import com.codahale.metrics.MetricRegistry;
import net.sf.xenqtt.client.MqttClient;
import net.sf.xenqtt.client.PublishMessage;
import net.sf.xenqtt.client.Subscription;
import net.sf.xenqtt.message.ConnectReturnCode;
import net.sf.xenqtt.message.QoS;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.buffers.Buffer;
import org.graylog2.plugin.buffers.BufferOutOfCapacityException;
import org.graylog2.plugin.buffers.ProcessingDisabledException;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.inputs.MisfireException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assume.assumeThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AsyncMQTTClientListenerTest {

    @Mock
    private Buffer processBuffer;
    @Mock
    private MessageInput messageInput;
    @Mock
    private MqttClient client;

    private AsyncMQTTClientListener listener;
    private MetricRegistry metricRegistry;

    @Before
    public void setUp() {
        when(messageInput.getUniqueReadableId()).thenReturn("test");

        metricRegistry = new MetricRegistry();
        listener = new AsyncMQTTClientListener(messageInput,
                Collections.singletonList(new Subscription("test", QoS.AT_LEAST_ONCE)), metricRegistry);
    }

    @Test
    public void testConnected() {
        listener.connected(client, ConnectReturnCode.ACCEPTED);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void connectedSubscribesToTopics() throws MisfireException {
        final MqttClient client = mock(MqttClient.class);
        listener.connected(client, ConnectReturnCode.ACCEPTED);

        final ArgumentCaptor<List> argumentCaptor = ArgumentCaptor.forClass(List.class);

        verify(client).subscribe((List<Subscription>) argumentCaptor.capture());
        assertThat(argumentCaptor.getValue().size(), is(1));

        final List<Subscription> subscriptions = argumentCaptor.getValue();
        assertThat(subscriptions.get(0).getTopic(), equalTo("test"));
        assertThat(subscriptions.get(0).getQos(), equalTo(QoS.AT_LEAST_ONCE));
    }

    @Test
    public void testSubscribedSuccessfully() {
        final Subscription[] subscriptions = new Subscription[]{
                new Subscription("test", QoS.EXACTLY_ONCE)
        };

        listener.subscribed(client, subscriptions, subscriptions, true);
    }

    @Test
    public void testSubscribedUnsuccessfully() {
        final Subscription[] requestedSubscriptions = new Subscription[]{
                new Subscription("test1", QoS.EXACTLY_ONCE),
                new Subscription("test2", QoS.EXACTLY_ONCE),
        };
        final Subscription[] grantedSubscriptions = new Subscription[]{
                new Subscription("test1", QoS.EXACTLY_ONCE)
        };

        listener.subscribed(client, requestedSubscriptions, grantedSubscriptions, false);
    }

    @Test
    public void testUnsubscribed() {
        listener.unsubscribed(client, new String[]{"test"});
    }

    @Test
    public void testPublished() {
        listener.published(client, new PublishMessage("test", QoS.EXACTLY_ONCE, ""));
    }

    @Test
    public void publishReceivedCountsIncomingMessages() {
        assumeThat(metricRegistry.getMeters().get("test.incomingMessages").getCount(), is(0l));
        listener.publishReceived(client, new PublishMessage("test", QoS.AT_LEAST_ONCE));

        assertThat(metricRegistry.getMeters().get("test.incomingMessages").getCount(), is(1l));
    }

    @Test
    public void publishReceivedCountsEmptyMessages() {
        assumeThat(metricRegistry.getMeters().get("test.incompleteMessages").getCount(), is(0l));
        listener.publishReceived(client, new PublishMessage("test", QoS.AT_LEAST_ONCE, (byte[]) null));
        listener.publishReceived(client, new PublishMessage("test", QoS.AT_LEAST_ONCE, new byte[0]));

        assertThat(metricRegistry.getMeters().get("test.incompleteMessages").getCount(), is(2l));
    }

    @Test
    public void publishReceivedCountsDuplicateMessages() {
        final PublishMessage message = new PublishMessage("test", QoS.EXACTLY_ONCE, "test");

        assumeThat(metricRegistry.getMeters().get("test.incompleteMessages").getCount(), is(0l));
        listener.publishReceived(client, message);

        //assertThat(metricRegistry.getMeters().get("test.incompleteMessages").getCount(), is(1l));
    }

    @Test
    public void publishReceivedCountsDiscardedMessages() throws BufferOutOfCapacityException, ProcessingDisabledException {
        final PublishMessage message =
                new PublishMessage("test", QoS.AT_LEAST_ONCE, "");

        assumeThat(metricRegistry.getMeters().get("test.incompleteMessages").getCount(), is(0l));
        listener.publishReceived(client, message);

        assertThat(metricRegistry.getMeters().get("test.incompleteMessages").getCount(), is(1l));
    }

    @Test
    public void publishReceivedCountsProcessedMessages() throws BufferOutOfCapacityException, ProcessingDisabledException {
        final PublishMessage message = new PublishMessage("test", QoS.AT_LEAST_ONCE, "{\"version\":\"1.1\", \"message\":\"test\"}");

        assumeThat(metricRegistry.getMeters().get("test.incompleteMessages").getCount(), is(0l));
        assumeThat(metricRegistry.getMeters().get("test.processedMessages").getCount(), is(0l));

        listener.publishReceived(client, message);

        assertThat(metricRegistry.getMeters().get("test.incompleteMessages").getCount(), is(0l));
        assertThat(metricRegistry.getMeters().get("test.processedMessages").getCount(), is(1l));
    }

    @Test
    public void testDisconnected() {
        listener.disconnected(client, null, false);
    }
}