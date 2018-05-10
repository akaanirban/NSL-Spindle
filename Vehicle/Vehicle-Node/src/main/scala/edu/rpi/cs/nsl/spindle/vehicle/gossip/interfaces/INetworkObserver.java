package edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.util.MessageStatus;

import java.util.UUID;

/**
 * The network observer, along with the INetworkSender, helps build layers of the code.
 */
public interface INetworkObserver {
    /**
     * Something this is observing received a message.
     *
     * @param sender
     * @param message
     */
    void OnNetworkActivity(String sender, Object message);

    /**
     * Something this is observing received a message status.
     *
     * @param messageId
     * @param status
     */
    void OnMessageStatus(UUID messageId, MessageStatus status);
}
