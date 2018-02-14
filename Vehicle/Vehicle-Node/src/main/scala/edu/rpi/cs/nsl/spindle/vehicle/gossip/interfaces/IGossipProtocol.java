package edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.network.ConnectionMap;

public interface IGossipProtocol extends INetworkObserver, Runnable {

    void SetGossip(IGossip gossip);
    IGossip GetGossip();

    void SetNetwork(INetworkSender sender);

    // TODO: make logical connection map
    void SetConnectionMap(ConnectionMap connectionMap);

    void LeadGossip();

    void Stop();
}
