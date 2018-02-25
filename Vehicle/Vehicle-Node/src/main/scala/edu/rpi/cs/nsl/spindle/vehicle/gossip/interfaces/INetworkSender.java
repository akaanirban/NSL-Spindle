package edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces;

public interface INetworkSender {
	/**
	 * Send a message async-ly along the network
	 * @param target
	 * @param message
	 */
	public void Send(String target, IGossipMessageData message);
}
