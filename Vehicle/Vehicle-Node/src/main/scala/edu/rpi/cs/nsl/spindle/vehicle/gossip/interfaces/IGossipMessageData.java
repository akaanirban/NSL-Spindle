package edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces;

import java.io.Serializable;
import java.util.UUID;

/**
 * A Message has an arbitrary data object and a UUID.
 */
public interface IGossipMessageData extends Serializable {
    Object GetData();

    UUID GetUUID();
}
