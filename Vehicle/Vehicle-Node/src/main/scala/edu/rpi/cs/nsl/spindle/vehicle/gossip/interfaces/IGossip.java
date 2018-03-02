package edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces;

/**
 * The gossip class represents the data for a gossip protocol.
 * Provides messages for leading or following, handles messages.
 * State should not change until Abort/Commit called.
 */
public interface IGossip {

    /**
     * Message when starting a round on this node.
     *
     * @return
     */
    IGossipMessageData GetLeadGossipMessage();

    /**
     * Gossip message but did not start a round on this node.
     *
     * @return
     */
    IGossipMessageData GetGossipMessage();

    /**
     * Process a message
     *
     * @param sender
     * @param message
     * @return
     */
    boolean HandleUpdateMessage(String sender, Object message);

    /**
     * Abort the gossip round, g(t+1) = g(t)
     */
    void Abort();

    /**
     * Commits the gosisp round, g(t+1) = f(g(t), ...)
     */
    void Commit();

    /**
     * Returns the committed value object for gossip
     */
    Object GetValue();
}
