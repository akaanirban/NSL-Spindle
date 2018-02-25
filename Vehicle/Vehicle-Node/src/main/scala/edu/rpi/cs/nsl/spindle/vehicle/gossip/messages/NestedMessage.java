package edu.rpi.cs.nsl.spindle.vehicle.gossip.messages;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;

import java.util.UUID;

public class NestedMessage implements IGossipMessageData {
    protected UUID m_uuid;
    protected IGossipMessageData m_message;

    public NestedMessage(IGossipMessageData message) {
        this.m_message = message;
        this.m_uuid = message.getUUID();
    }

    @Override
    public Object getData() {
        return m_message;
    }

    @Override
    public UUID getUUID() {
        return m_uuid;
    }

    @Override
    public String toString() {
        return "[id=" + m_uuid + "msg=" + m_message + "]";
    }
}
