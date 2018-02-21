package gossip.gossip;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.gossip.Consensus;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.gossip.PushSum;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.messages.ValueWeightMessageData;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ConsensusGossipTest {

    protected Consensus gossip;

    double startValue = 1.0;
    double expectedValue = 1.5;

    // value 2.0, 1.0
    protected ValueWeightMessageData msg;
    protected double epsilon = 0.000001;

    @Before
    public void setUp() {
        gossip = new Consensus(1.0, 1.0);
        msg = new ValueWeightMessageData(2.0, 1.0);
    }

    public void testGetValue() {
        assertEquals(startValue, (double) gossip.GetValue(), epsilon);
    }

    @Test
    public void testUpdateCommit() {
        gossip.HandleUpdateMessage("", msg);
        gossip.Commit();
        assertEquals(expectedValue, (double) gossip.GetValue(), epsilon);
    }

    @Test
    public void testUpdateAbort() {
        gossip.HandleUpdateMessage("", msg);
        gossip.Abort();
        assertEquals(startValue, (double) gossip.GetValue(), epsilon);
    }

    @Test
    public void testGetCommit() {
        gossip.GetGossipMessage();
        gossip.Commit();
        assertEquals(startValue, (double) gossip.GetValue(), epsilon);
    }

    @Test
    public void testGetAbort() {
        gossip.GetGossipMessage();
        gossip.Abort();
        assertEquals(startValue, (double) gossip.GetValue(), epsilon);
    }
}
