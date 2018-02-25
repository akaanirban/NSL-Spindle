package edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.MessageStatus;

import java.util.UUID;

public interface INetworkObserver {
	public void OnNetworkActivity(String sender, Object message);
	
	/**
	 *  @param messageId
	 * @param status
	 */
	public void OnMessageStatus(UUID messageId, MessageStatus status);
}
