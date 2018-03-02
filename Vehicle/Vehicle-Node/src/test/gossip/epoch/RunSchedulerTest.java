package gossip.epoch;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.epoch.IntervalHelper;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunSchedulerTest {
    Logger logger = LoggerFactory.getLogger(this.getClass());

    IntervalHelper scheduler;

    @Before
    public void setup() {
        scheduler = new IntervalHelper(5);
    }

    @Test
    @Ignore
    public void testBasic() {
        for (int i = 0; i < 40; i++) {
            System.out.println(scheduler.GetNext());
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
