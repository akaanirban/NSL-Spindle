package edu.rpi.cs.nsl.spindle.vehicle.gossip.messages;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipSendMessage;

public class ConsensusFollowResponse extends NestedMessage {

    public ConsensusFollowResponse(IGossipMessageData message) {
        super(message);
    }
}
