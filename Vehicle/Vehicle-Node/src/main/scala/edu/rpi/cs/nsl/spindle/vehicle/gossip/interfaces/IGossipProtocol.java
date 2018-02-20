package edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces;

public interface IGossipProtocol extends INetworkObserver, Runnable {

    void SetGossip(IGossip gossip);
    IGossip GetGossip();

    void SetNetwork(INetworkSender sender);

    // TODO: make logical connection map
    void SetConnectionMap(ILogicalNetwork logicalNetwork);

    // iteration logic should be in here, made public to make testing easier
    void doIteration();

    void LeadGossip();

    void Stop();
}
