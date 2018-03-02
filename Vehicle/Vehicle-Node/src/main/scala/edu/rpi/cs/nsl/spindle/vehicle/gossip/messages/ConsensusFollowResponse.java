package edu.rpi.cs.nsl.spindle.vehicle.gossip.messages;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;

import java.util.UUID;

public class ConsensusFollowResponse extends NestedMessage {

    public ConsensusFollowResponse(IGossipMessageData message, UUID whichLead) {
        super(message);
        m_whichLead = whichLead;
    }

    protected UUID m_whichLead;

    public UUID GetLeadUUID() {
        return m_whichLead;
    }

    @Override
    public String toString() {
        return "[type=cfr following=" + m_whichLead + super.toString() + "]";
    }
}
