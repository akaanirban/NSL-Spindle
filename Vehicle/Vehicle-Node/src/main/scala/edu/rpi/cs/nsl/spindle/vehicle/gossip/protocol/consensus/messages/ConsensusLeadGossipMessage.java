package edu.rpi.cs.nsl.spindle.vehicle.gossip.protocol.consensus.messages;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.messages.NestedMessage;

/**
 * Indicates that a node want to lead and wants the receiver to follow
 */
public class ConsensusLeadGossipMessage extends NestedMessage {
    public ConsensusLeadGossipMessage(IGossipMessageData message) {
        super(message);
    }

    @Override
    public String toString() {
        return "[type=clm" + super.toString() + "]";
    }
}
