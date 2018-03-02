package gossip.testingUtils;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;
import org.mockito.ArgumentMatcher;

public class MessageMatcher implements ArgumentMatcher<IGossipMessageData> {
    protected IGossipMessageData m_message;

    public MessageMatcher(IGossipMessageData msg) {
        this.m_message = msg;
    }

    public boolean matches(IGossipMessageData other) {
        if (other instanceof IGossipMessageData) {
            return ((IGossipMessageData) other).GetData() == m_message;
        }

        return false;
    }
}
