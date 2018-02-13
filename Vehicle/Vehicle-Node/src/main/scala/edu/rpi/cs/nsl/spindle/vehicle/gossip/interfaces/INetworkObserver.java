package edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.MessageStatus;

public interface INetworkObserver {
	public void OnNetworkActivity(String sender, Object message);
	
	/**
	 * 
	 * @param target
	 * @param status
	 */
	public void OnMessageStatus(String target, MessageStatus status);
}
