package edu.rpi.cs.nsl.spindle.vehicle.gossip.protocol.consensus.messages;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.messages.BaseMessage;

import java.util.UUID;

/**
 * indicates that there is some conflict and the receiver should abort the gossip
 */
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
