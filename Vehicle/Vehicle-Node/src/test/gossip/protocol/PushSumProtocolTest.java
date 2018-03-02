package gossip.protocol;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.MessageStatus;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossip;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.ILogicalNetwork;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkSender;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.messages.ValueWeightMessageData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.protocol.PushSumProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;


public class PushSumProtocolTest {

    PushSumProtocol protocol;

    @Mock
    IGossip gossip;
    @Mock
    INetworkSender sender;
    @Mock
    ILogicalNetwork logicalNetwork;

    ValueWeightMessageData sendMessage;

    @Before
    public void doFirst() {
        MockitoAnnotations.initMocks(this);

        sendMessage = new ValueWeightMessageData(1.0, 1.0);

        protocol = new PushSumProtocol("1");
        protocol.SetGossip(gossip);
        protocol.SetNetwork(sender);
        protocol.SetConnectionMap(logicalNetwork);
    }

    @After
    public void after() {
        verifyNoMoreInteractions(sender, gossip, logicalNetwork);
    }

    @Test
    public void testOnReceiveMessage() {
        protocol.OnNetworkActivity("1", sendMessage);
        protocol.doIteration();

        verify(gossip).HandleUpdateMessage(eq("1"), eq(sendMessage));
        verify(gossip).Commit();
    }

    @Test
    public void testLeadWithSelf() {
        String target = "1";
        when(logicalNetwork.ChooseRandomTarget()).thenReturn(target);

        protocol.LeadGossip();
        protocol.doIteration();
        verify(logicalNetwork, times(1)).ChooseRandomTarget();

    }

    @Test
    public void testLeadNoStatus() {
        String target = "2";
        when(gossip.GetLeadGossipMessage()).thenReturn(sendMessage);
        when(logicalNetwork.ChooseRandomTarget()).thenReturn(target);

        protocol.LeadGossip();
        protocol.doIteration();
        verify(gossip, times(1)).GetLeadGossipMessage();
        verify(logicalNetwork, times(1)).ChooseRandomTarget();
        verify(sender, times(1)).Send(eq(target), eq(sendMessage));
    }

    @Test
    public void testLeadStatusGood() {
        String target = "2";
        when(gossip.GetLeadGossipMessage()).thenReturn(sendMessage);
        when(logicalNetwork.ChooseRandomTarget()).thenReturn(target);

        protocol.LeadGossip();
        protocol.doIteration();

        protocol.OnMessageStatus(sendMessage.getUUID(), MessageStatus.GOOD);
        protocol.doIteration();

        verify(gossip, times(1)).Commit();
        verify(gossip, times(1)).GetLeadGossipMessage();
        verify(logicalNetwork, times(1)).ChooseRandomTarget();
        verify(sender, times(1)).Send(eq(target), eq(sendMessage));
    }

    @Test
    public void testLeadStatusBad() {
        String target = "2";
        when(gossip.GetLeadGossipMessage()).thenReturn(sendMessage);
        when(logicalNetwork.ChooseRandomTarget()).thenReturn(target);

        protocol.LeadGossip();
        protocol.doIteration();

        protocol.OnMessageStatus(sendMessage.getUUID(), MessageStatus.BAD);
        protocol.doIteration();

        verify(gossip, times(1)).Abort();
        verify(gossip, times(1)).GetLeadGossipMessage();
        verify(logicalNetwork, times(1)).ChooseRandomTarget();
        verify(sender, times(1)).Send(eq(target), eq(sendMessage));
    }
}
