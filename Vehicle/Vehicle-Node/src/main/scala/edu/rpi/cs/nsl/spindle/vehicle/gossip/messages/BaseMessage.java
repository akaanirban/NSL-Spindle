package edu.rpi.cs.nsl.spindle.vehicle.gossip.messages;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;

import java.util.UUID;

/**
 * Provides uuid, toString method.
 */
public abstract class BaseMessage implements IGossipMessageData {

    protected UUID m_uuid;

    public BaseMessage() {
        m_uuid = UUID.randomUUID();
    }

    @Override
    public UUID GetUUID() {
        return m_uuid;
    }

    @Override
    public String toString() {
        return m_uuid.toString();
    }
}
