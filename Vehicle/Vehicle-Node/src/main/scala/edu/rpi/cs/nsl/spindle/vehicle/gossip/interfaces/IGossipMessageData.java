package edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces;

import java.io.Serializable;

/**
 * Contains the raw message data
 */
public interface IGossipMessageData extends Serializable {
    Object getData();
}
