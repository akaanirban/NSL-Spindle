package edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces;

/**
 * Network sender processes messages when Send is called.
 */
public interface INetworkSender {
    /**
     * Send a message along the network
     *
     * @param target
     * @param message
     */
    void Send(String target, IGossipMessageData message);
}
