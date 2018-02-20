package gossip.protocol;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.*;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.protocol.PushSumProtocol;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class PushSum {

    PushSumProtocol protocol;

    @Mock IGossip gossip;
    @Mock INetworkSender sender;
    @Mock ILogicalNetwork logicalNetwork;
    @Mock IGossipMessageData sendMessage;

    protected TestMessage BuildTestMessage() {
        return new TestMessage();
    }

    @Before
    public void doFirst() {
        MockitoAnnotations.initMocks(this);
        protocol = new PushSumProtocol("1");
        protocol.SetGossip(gossip);
        protocol.SetNetwork(sender);
        protocol.SetConnectionMap(logicalNetwork);
    }

    @Test
    public void testOnReceiveMessage() {
        TestMessage msg = BuildTestMessage();
        protocol.OnNetworkActivity("1", msg);
        protocol.doIteration();

        verify(gossip).HandleUpdateMessage(eq("1"), eq(msg));
    }

    @Test
    public void testLeadWithSelf() {
        String target = "1";
        when(gossip.GetLeadGossipMessage()).thenReturn(sendMessage);
        when(logicalNetwork.ChooseRandomTarget()).thenReturn(target);

        protocol.LeadGossip();
        protocol.doIteration();
        verify(sender, times(0)).Send(anyString(), anyObject());
    }

    @Test
    public void testLead() {
        String target = "2";
        when(gossip.GetLeadGossipMessage()).thenReturn(sendMessage);
        when(logicalNetwork.ChooseRandomTarget()).thenReturn(target);

        protocol.LeadGossip();
        protocol.doIteration();
        verify(sender, times(1)).Send(eq(target), eq(sendMessage));
    }

    public class TestMessage { }
}
