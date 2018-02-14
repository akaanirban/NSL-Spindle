package edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces;

public interface IGossip<DataT> {

    /**
     * Called when we want to start a round of gossip on this host
     * @return
     */
    IGossipMessageData GetLeadGossipMessage();
    IGossipMessageData GetGossipMessage();
    boolean HandleUpdateMessage(String sender, Object message);

    void Reset();

    DataT GetValue();
}
