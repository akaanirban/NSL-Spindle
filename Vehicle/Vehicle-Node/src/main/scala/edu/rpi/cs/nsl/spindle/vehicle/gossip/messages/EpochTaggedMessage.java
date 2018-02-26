package edu.rpi.cs.nsl.spindle.vehicle.gossip.messages;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.epoch.Epoch;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;

public class EpochTaggedMessage extends NestedMessage {
    protected Epoch m_epoch;

    public EpochTaggedMessage(IGossipMessageData message, Epoch epoch) {
        super(message);
        this.m_epoch = epoch;
    }

    public Epoch GetEpoch() {
        return m_epoch;
    }
}
