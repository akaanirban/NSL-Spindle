package edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces;

/**
 * Gossip Protocol manages interaction with an IGossip.
 */
public interface IGossipProtocol extends INetworkObserver, Runnable {

    void SetGossip(IGossip gossip);

    IGossip GetGossip();

    void SetNetwork(INetworkSender sender);

    void SetConnectionMap(ILogicalNetwork logicalNetwork);

    /**
     * Run an iteration of the protocol. This is made public to make testing easier.
     */
    void DoIteration();

    /**
     * Request the protocol leads a round
     */
    void LeadGossip();

    /**
     * Request that the protocol stop running.
     */
    void Stop();
}
