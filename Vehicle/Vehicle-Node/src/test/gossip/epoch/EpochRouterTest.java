package gossip.epoch;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.epoch.Epoch;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.epoch.EpochRouter;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkObserver;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkSender;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.messages.EpochTaggedMessage;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.messages.QueryTaggedMessage;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.query.QueryTagger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Instant;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

public class EpochRouterTest {
    EpochRouter router;

    @Mock
    INetworkSender sender;

    @Mock
    INetworkObserver observer;

    @Mock
    IGossipMessageData messageData1;
    @Mock
    IGossipMessageData messageData2;

    Epoch startEpoch;
    Epoch futureEpoch;

    EpochTaggedMessage goodMessage;
    EpochTaggedMessage futureMessage;

    String id = "1";

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        router = new EpochRouter(sender);
        router.SetObserver(observer);

        Instant now = Instant.now();
        startEpoch = new Epoch(now);
        futureEpoch = new Epoch(now.plusSeconds(1));

        router.SetEpoch(startEpoch);

        when(messageData1.getUUID()).thenReturn(UUID.randomUUID());
        when(messageData2.getUUID()).thenReturn(UUID.randomUUID());

        goodMessage = new EpochTaggedMessage(messageData1, startEpoch);
        futureMessage = new EpochTaggedMessage(messageData2, futureEpoch);
    }

    @After
    public void after() {
        verifyNoMoreInteractions(sender, observer);
    }

    @Test
    public void testSendMessage() {
        router.Send(id, messageData1);

        // check that the observer got a query tagged message
        verify(sender, times(1)).Send(eq(id), isA(EpochTaggedMessage.class));
    }

    @Test
    public void testGetGoodMessage() {
        router.OnNetworkActivity(id, goodMessage);

        verify(observer, times(1)).OnNetworkActivity(eq(id), eq(messageData1));
    }
}
