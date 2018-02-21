package edu.rpi.cs.nsl.spindle.vehicle.gossip.messages;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipSendMessage;

public class ConsensusFollowResponse implements IGossipMessageData {
    protected IGossipMessageData m_message;

    public ConsensusFollowResponse(IGossipMessageData message) {
        this.m_message = message;
    }

    @Override
    public Object getData() {
        return m_message;
    }
}
