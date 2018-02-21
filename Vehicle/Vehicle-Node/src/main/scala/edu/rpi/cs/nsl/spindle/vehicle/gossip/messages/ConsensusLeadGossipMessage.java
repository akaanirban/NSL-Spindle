package edu.rpi.cs.nsl.spindle.vehicle.gossip.messages;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;

public class ConsensusLeadGossipMessage extends NestedMessage {
    public ConsensusLeadGossipMessage(IGossipMessageData message) {
        super(message);
    }
}
