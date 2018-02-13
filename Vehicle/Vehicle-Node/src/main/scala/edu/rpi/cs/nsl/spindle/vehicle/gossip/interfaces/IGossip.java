package edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces;

public interface IGossip<DataT> {

    /**
     * Called when we want to start a round of gossip on this host
     * @return
     */
    IGossipSendMessage GetStartGossipMessage();
    IGossipSendMessage GetGossipMessage();
    boolean HandleUpdateMessage(String sender, IGossipMessageData message);

    void Reset();

    DataT GetValue();
}
