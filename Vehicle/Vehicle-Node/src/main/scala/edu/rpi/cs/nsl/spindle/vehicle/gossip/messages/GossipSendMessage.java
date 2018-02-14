package edu.rpi.cs.nsl.spindle.vehicle.gossip.messages;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipSendMessage;

import java.util.List;

public class GossipSendMessage implements IGossipSendMessage {
    protected List<String> m_targets;
    protected IGossipMessageData m_messageData;

    public GossipSendMessage(List<String> targets, IGossipMessageData messageData) {
        m_targets = targets;
        m_messageData = messageData;
    }

    @Override
    public List<String> GetTargets() {
        return m_targets;
    }

    @Override
    public IGossipMessageData getMessage() {
        return m_messageData;
    }
}
