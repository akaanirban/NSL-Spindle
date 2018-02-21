package edu.rpi.cs.nsl.spindle.vehicle.gossip.messages;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;

// when a node doesn't want to gossip
public class ConsensusNoGossipResponse implements IGossipMessageData {
    @Override
    public Object getData() {
        return null;
    }
}
