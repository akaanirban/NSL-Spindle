package edu.rpi.cs.nsl.spindle.vehicle.gossip.messages;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;

import java.util.UUID;

/**
 * Nested message is a message that contains another message. It reuses that other message's uuid.
 */
public class NestedMessage implements IGossipMessageData {
    protected UUID m_uuid;
    protected IGossipMessageData m_message;

    public NestedMessage(IGossipMessageData message) {
        this.m_message = message;
        this.m_uuid = message.GetUUID();
    }

    @Override
    public Object GetData() {
        return m_message;
    }

    @Override
    public UUID GetUUID() {
        return m_uuid;
    }

    @Override
    public String toString() {
        return "[id=" + m_uuid + "msg=" + m_message + "]";
    }
}
