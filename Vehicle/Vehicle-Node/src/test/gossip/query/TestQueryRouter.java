package gossip.query;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.MessageStatus;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipProtocol;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkSender;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.messages.ConsensusLeadGossipMessage;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.messages.QueryTaggedMessage;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.query.Query;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.query.QueryRouter;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.query.QueryTagger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.UUID;

import static org.mockito.Mockito.*;

public class TestQueryRouter {

    QueryRouter router;

    @Mock
    INetworkSender sender;

    @Mock
    IGossipProtocol observer1;

    @Mock
    IGossipProtocol observer2;

    @Mock
    IGossipMessageData messageData1;
    @Mock
    IGossipMessageData messageData2;

    ConsensusLeadGossipMessage message1;
    ConsensusLeadGossipMessage message2;

    UUID uuid1 = UUID.randomUUID();
    UUID uuid2 = UUID.randomUUID();
    UUID badUUID = UUID.randomUUID();


    Query query1;
    Query query2;
    Query query3;

    QueryTaggedMessage taggedMessage1;
    QueryTaggedMessage taggedMessage2;

    QueryTaggedMessage badMessage;


    String targetID = "1";

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        query1 = new Query("sum", "items");
        query2 = new Query("avg", "speed");
        query3 = new Query("bad", "bad");

        when(messageData1.getUUID()).thenReturn(uuid1);
        when(messageData2.getUUID()).thenReturn(uuid2);

        message1 = new ConsensusLeadGossipMessage(messageData1);
        message2 = new ConsensusLeadGossipMessage(messageData2);

        router = new QueryRouter();
        router.SetNetwork(sender);

        // add both as observers
        router.InsertOrReplace(query1, observer1);
        router.InsertOrReplace(query2, observer2);


        taggedMessage1 = new QueryTaggedMessage(message1, query1);
        taggedMessage2 = new QueryTaggedMessage(message2, query2);

        badMessage = new QueryTaggedMessage(message1, query3);
    }

    @After
    public void after() {
        verify(observer1,times(1)).SetNetwork(isA(QueryTagger.class));
        verify(observer2,times(1)).SetNetwork(isA(QueryTagger.class));
        verifyNoMoreInteractions(sender, observer1, observer2);
    }

    void sendBothMessages() {
        router.Send(targetID, taggedMessage1);
        router.Send(targetID, taggedMessage2);
        verify(sender, times(1)).Send(eq(targetID), eq(taggedMessage1));
        verify(sender, times(1)).Send(eq(targetID), eq(taggedMessage2));
    }

    @Test
    public void testSendTaggedMessage() {
        // just send a simple tagged message
        router.Send(targetID, taggedMessage1);

        verify(sender, times(1)).Send(eq(targetID), eq(taggedMessage1));
    }

    @Test
    public void testSendTaggedMessageBadQuery() {
        // should have no interactions
        router.Send(targetID, badMessage);
    }

    @Test
    public void testGetStatusGood() {
        sendBothMessages();

        // now give status for 1, only the first observer should get it
        router.OnMessageStatus(uuid1, MessageStatus.GOOD);

        verify(observer1, times(1)).OnMessageStatus(uuid1, MessageStatus.GOOD);
    }

    @Test
    public void testGetStatusDouble() {
        sendBothMessages();

        // now give status for 1, only the first observer should get it
        router.OnMessageStatus(uuid1, MessageStatus.GOOD);

        // second time shouldn't get response
        router.OnMessageStatus(uuid1, MessageStatus.GOOD);

        verify(observer1, times(1)).OnMessageStatus(uuid1, MessageStatus.GOOD);
    }

    @Test
    public void testGetStatusBadUUID() {
        // send messages, don't have any
        sendBothMessages();

        // shouldn't have any interactions after this
        router.OnMessageStatus(badUUID, MessageStatus.GOOD);
    }

    @Test
    public void testGetMessageGood() {
        router.OnNetworkActivity(targetID, taggedMessage1);

        verify(observer1, times(1)).OnNetworkActivity(eq(targetID), eq(message1));
    }

    @Test
    public void testGetMessageBadQuery() {
        router.OnNetworkActivity(targetID, badMessage);
    }
}
