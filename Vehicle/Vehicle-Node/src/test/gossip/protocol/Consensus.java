package gossip.protocol;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.MessageStatus;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossip;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.ILogicalNetwork;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkSender;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.messages.ConsensusFollowResponse;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.messages.ConsensusLeadGossipMessage;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.messages.ConsensusNoGossipResponse;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.protocol.ConsensusProtocol;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.protocol.PushSumProtocol;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

public class Consensus {
    ConsensusProtocol protocol;

    @Mock
    IGossip gossip;
    @Mock
    INetworkSender sender;
    @Mock
    ILogicalNetwork logicalNetwork;

    @Mock
    IGossipMessageData leadMsgData;

    @Mock
    IGossipMessageData responseMsgData;

    ConsensusLeadGossipMessage leadMsg;

    ConsensusFollowResponse responseMsg;

    @Mock
    ConsensusNoGossipResponse noGossipResponse;

    String selfId = "1";
    String otherId = "2";
    String otherId2 = "3";

    @Before
    public void doFirst() {
        MockitoAnnotations.initMocks(this);

        leadMsg = new ConsensusLeadGossipMessage(leadMsgData);
        responseMsg = new ConsensusFollowResponse(responseMsgData);

        protocol = new ConsensusProtocol(selfId);
        protocol.SetGossip(gossip);
        protocol.SetNetwork(sender);
        protocol.SetConnectionMap(logicalNetwork);
    }

    /**
     * cases:
     * leading gossip
     *  - good
     *  - get message from another
     *      - get follow message from gossip m_target
     *  - get another request to lead
     *  - fail status
     *  - follow fails?
     * follow gossip:
     *  - good
     *  - fail status
     */

    // does the send message side
    // doesn't verify anything
    void doSendLeadMessage() {
        // good lead gossip, send message, get response
        protocol.LeadGossip();

        when(gossip.GetLeadGossipMessage()).thenReturn(leadMsg);
        when(logicalNetwork.ChooseRandomTarget()).thenReturn(otherId);

        protocol.doIteration();
    }

    @Test
    public void testLeadGood() {
        doSendLeadMessage();
        verify(sender, times(0)).Send(otherId, leadMsg);

        // indicate good status and send
        protocol.OnMessageStatus(otherId, MessageStatus.GOOD);
        protocol.doIteration();

        // now get response message
        // TODO: is this really what we want? Or do we want to parse something out of it...
        protocol.OnNetworkActivity(otherId, responseMsg);
        protocol.doIteration();

        verify(gossip, times(1)).HandleUpdateMessage(eq(otherId), eq(responseMsgData));
        verify(gossip, times(1)).Commit();
        verify(gossip, times(1)).GetLeadGossipMessage();
        verify(gossip, never()).GetGossipMessage();
    }

    @Test
    public void testLeadGetGossipingMessage() {
        // try to gossip but received "already trying to gossip" response from m_target
        doSendLeadMessage();
        verify(sender, times(1)).Send(otherId, leadMsg);

        // indicate good status and send
        protocol.OnMessageStatus(otherId, MessageStatus.GOOD);
        protocol.doIteration();

        // m_target doesn't want to gossip
        protocol.OnNetworkActivity(otherId, noGossipResponse);
        protocol.doIteration();

        // now should be able to follow a separate m_target
        when(gossip.GetGossipMessage()).thenReturn(leadMsg);
        protocol.OnNetworkActivity(otherId2, leadMsg);
        protocol.doIteration();


        // expect it to send a follow message
        verify(sender).Send(otherId2, responseMsg);
    }

    @Test
    public void testLeadGetFollowMessage() {
        // trying to gossip but receive follow message from m_target, should gossip fine
        doSendLeadMessage();
        verify(sender, times(1)).Send(otherId, leadMsg);

        // indicate good status and send
        protocol.OnMessageStatus(otherId, MessageStatus.GOOD);
        protocol.doIteration();

        // now receive a message for other to follow
        protocol.OnNetworkActivity(otherId, leadMsg);

        // they should have our message, just commit with their message
        // TODO: make commit mechanism
    }

    @Test
    public void testLeadSecondLeadRequest() {
        // trying to gossip but get a second lead request from manager
        doSendLeadMessage();
        verify(sender, times(1)).Send(otherId, leadMsg);

        // indicate good status and send
        protocol.OnMessageStatus(otherId, MessageStatus.GOOD);
        protocol.doIteration();

        // waiting, then get a request to lead
        protocol.LeadGossip();
        protocol.doIteration();

        // expect that the sender gets no more, should be from above
    }

    @Test
    public void testLeadFailStatus() {
        doSendLeadMessage();

        // try to lead but get failure message
        protocol.OnMessageStatus(otherId, MessageStatus.BAD);
        protocol.doIteration();

        // now should be able to follow
        protocol.OnNetworkActivity(otherId, leadMsg);
    }

    @Test
    public void testFollowGood() {
        // receive a follow message and work fine
        protocol.OnNetworkActivity(otherId, leadMsg);
        protocol.doIteration();

        protocol.OnMessageStatus(otherId, MessageStatus.GOOD);
        protocol.doIteration();

        // now should ask for a response message and send it
        when(gossip.GetGossipMessage()).thenReturn(responseMsg);
        verify(gossip, times(1)).HandleUpdateMessage(eq(otherId), eq(leadMsgData));
        verify(gossip, times(1)).GetGossipMessage();
        verify(sender, times(1)).Send(eq(otherId), isA(ConsensusFollowResponse.class));
        verify(gossip, times(1)).Commit();
    }

    @Test
    public void testFollowFailStatus() {
        // receive follow message send status then get fail status
        // receive a follow message and work fine
        protocol.OnNetworkActivity(otherId, leadMsg);
        protocol.doIteration();

        protocol.OnMessageStatus(otherId, MessageStatus.BAD);
        protocol.doIteration();

        // now should ask for a response message and send it
        when(gossip.GetGossipMessage()).thenReturn(responseMsg);
        verify(gossip, times(1)).HandleUpdateMessage(eq(otherId), eq(leadMsgData));
        verify(gossip, times(1)).GetGossipMessage();
        verify(sender, times(1)).Send(eq(otherId), isA(ConsensusFollowResponse.class));
        verify(gossip, times(1)).Abort();
    }
}
