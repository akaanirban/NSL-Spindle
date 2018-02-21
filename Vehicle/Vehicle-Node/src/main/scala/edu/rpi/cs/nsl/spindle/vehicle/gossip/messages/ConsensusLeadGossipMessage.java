package edu.rpi.cs.nsl.spindle.vehicle.gossip.messages;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;

public class ConsensusLeadGossipMessage implements IGossipMessageData {
    protected IGossipMessageData m_message;

    public ConsensusLeadGossipMessage(IGossipMessageData message) {
        this.m_message = message;
    }

    @Override
    public Object getData() {
        return m_message;
    }
}
