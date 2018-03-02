package edu.rpi.cs.nsl.spindle.vehicle.gossip.messages;

import java.util.UUID;

// when a node doesn't want to gossip
public class ConsensusNoGossipResponse extends BaseMessage {
    public ConsensusNoGossipResponse(UUID whichLead) {
        m_whichLead = whichLead;
    }

    @Override
    public Object GetData() {
        return null;
    }

    protected UUID m_whichLead;

    public UUID GetLeadUUID() {
        return m_whichLead;
    }

    @Override
    public String toString() {
        return "[type=noG" + super.toString() + "]";
    }
}
