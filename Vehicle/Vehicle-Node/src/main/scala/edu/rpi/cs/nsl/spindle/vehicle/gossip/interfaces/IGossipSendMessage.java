package edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces;

import java.util.List;

/**
 * wrapper on the message sending, contains a list of targets and the raw message to send
 */
public interface IGossipSendMessage {
    /**
     *
     * @return list of targets for this message
     */
    List<String> GetTargets();

    /**
     *
     * @return the underlying data for this message
     */
    IGossipMessageData getMessage();
}
