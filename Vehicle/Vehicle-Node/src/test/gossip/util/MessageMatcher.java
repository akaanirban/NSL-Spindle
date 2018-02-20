package gossip.util;

import com.intellij.openapi.ui.Messages;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossip;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.mockito.ArgumentMatcher;

public class MessageMatcher implements ArgumentMatcher<IGossipMessageData> {
    protected IGossipMessageData m_message;
    public MessageMatcher(IGossipMessageData msg) {
        this.m_message = msg;
    }

    public boolean matches(IGossipMessageData other) {
        if(other instanceof IGossipMessageData) {
            return ((IGossipMessageData) other).getData() == m_message;
        }

        return false;
    }
}
