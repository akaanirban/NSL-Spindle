package edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces;

import java.io.Serializable;
import java.util.UUID;

/**
 * Contains the raw message data
 */
public interface IGossipMessageData extends Serializable {
    Object getData();
    UUID getUUID();
}
