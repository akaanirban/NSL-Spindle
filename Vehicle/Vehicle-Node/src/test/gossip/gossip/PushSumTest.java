package gossip.gossip;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.gossip.PushSum;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.messages.ValueWeightMessageData;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PushSumTest {

    protected PushSum pushSum;

    double startValue = 1.0;
    double expectedValue = 1.5;

    // value 2.0, 1.0
    protected ValueWeightMessageData msg;
    protected double epsilon = 0.000001;

    @Before
    public void setUp() {
        pushSum = new PushSum(1.0, 1.0);
        msg = new ValueWeightMessageData(2.0, 1.0);
    }

    public void testGetValue() {
        assertEquals(startValue, (double) pushSum.GetValue(), epsilon);
    }

    @Test
    public void testUpdateCommit() {
        pushSum.HandleUpdateMessage("", msg);
        pushSum.Commit();
        assertEquals(expectedValue, (double) pushSum.GetValue(), epsilon);
    }

    @Test
    public void testUpdateAbort() {
        pushSum.HandleUpdateMessage("", msg);
        pushSum.Abort();
        assertEquals(startValue, (double) pushSum.GetValue(), epsilon);
    }

    @Test
    public void testGetCommit() {
        pushSum.GetGossipMessage();
        pushSum.Commit();
        assertEquals(startValue, (double) pushSum.GetValue(), epsilon);
    }

    @Test
    public void testGetAbort() {
        pushSum.GetGossipMessage();
        pushSum.Abort();
        assertEquals(startValue, (double) pushSum.GetValue(), epsilon);
    }
}
