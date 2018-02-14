package edu.rpi.cs.nsl.spindle.vehicle.gossip.__old__;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.MessageStatus;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.gossip.PushSumData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkObserver;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkSender;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class NormalGossip implements INetworkObserver {
	
	protected long windowSizeMS;
	protected INetworkSender sender;
	protected PushSumData value;
	
	protected boolean isGossiping;
	protected MessageStatus sendStatus;
	protected boolean receiveStatus;
	protected String gossipTarget;
	protected PushSumData receivedValue;
	//protected Semaphore mux;
	protected Lock mux;
	
	protected Instant startTime;
	
	public NormalGossip() {
		this.value = new PushSumData(0.0,  0.0);
		//this.mux = new Semaphore(1);
		this.mux = new ReentrantLock();
		this.startTime = Instant.now();
	}
	
	public void SetWeight(double weight) {
		value.weight = weight;
	}
	
	public double GetWeigth() {
		return this.value.weight;
	}
	
	public void SetValue(Double value) {
		this.value.value = value;
	}
	
	public double GetValue() {
		return this.value.value;
	}
	
	public void SetNetworkSender(INetworkSender sender) {
		this.sender = sender;
	}
	
	/**
	 * 
	 * @param windowSize
	 *            measured in MS
	 */
	public void setWindowSizeMS(long windowSize) {
		this.windowSizeMS = windowSize;
	}

	protected void startTimer() {
		startTime = Instant.now();
	}
	
	protected boolean expired() {
		Instant now = Instant.now();
		long gap = ChronoUnit.MILLIS.between(startTime, now);
		if(gap > windowSizeMS) {
			return true;
		}
		
		return false;
	}
	
	public void LeadGossip(String target) {
		//this.mux.unlock();
		this.mux.lock();
		
		// don't try to lead if we're already gossiping
		if(isGossiping && !expired()) {
			System.out.println("already gossiping: " + this);
			this.mux.unlock();
			return;
		}
		
		resetGossipState();
		startTimer();
		isGossiping = true;
		gossipTarget = target;
		
		sender.Send(target, value.GetHalf());
		System.out.println("lead gossip: " + this);
		this.mux.unlock();
	}
	
	protected void FollowGossip(String target, PushSumData otherValue) {
		System.out.println("follow gossip: " + this);

		this.mux.lock();
		
		// don't try to gossip if we're already gossiping
		if(isGossiping && !expired()) {
			System.out.println("can't follow, already gossiping, this is bad: " + this);
			this.mux.unlock();
			return;
		}
		
		resetGossipState();
		startTimer();
		isGossiping = true;
		gossipTarget = target;
		receivedValue = otherValue;
		receiveStatus = true;
		
		System.out.println("good follow: " + this);
		sender.Send(target, value.GetHalf());
		
		this.mux.unlock();
	}
	
	protected void resetGossipState() {
		sendStatus = MessageStatus.WAITING;
		receivedValue = new PushSumData(0.0, 0.0);
		isGossiping = false;
		gossipTarget = "";
		receiveStatus = false;
	}
	
	protected void TryUpdateGossip() {
		System.out.println("trying to update the gossip!");
		this.mux.lock();
		
		if(isGossiping && expired()) {
			System.out.println("update fail: expiring in update");
			resetGossipState();
			this.mux.unlock();
			return;
		}
		
		if(!isGossiping) {
			this.mux.unlock();
			System.out.println("update fail: not gossiping");
			return;
		}	
		
		if(!receiveStatus) {
			this.mux.unlock();
			System.out.println("update fail: haven't recv'd");
			return;
		}
		
		if(sendStatus == MessageStatus.BAD) {
			resetGossipState();
			System.out.println("update fail: bad gossip, resetting");
			this.mux.unlock();
			
			return;
		}
		
		if(sendStatus == MessageStatus.WAITING) {
			this.mux.unlock();
			System.out.println("update fail: waiting for send receipt");
			return;
		}
		
		// gossiping, and recv'd and good send
		System.out.printf("good gossip adding %f and %f",
				value.value, receivedValue.value);
		
		value.Update(receivedValue);
		resetGossipState();
		
		this.mux.unlock();
	}
	
	protected void updateGossiping() {
		if(isGossiping && expired()) {
			this.isGossiping = false;
			resetGossipState();
		}
	}
	@Override
	public void OnNetworkActivity(String sender, Object message) {
		// try to cast, then try to respond
		PushSumData otherVal = (PushSumData) message;

		this.mux.lock();
		
		updateGossiping();
		
		if(isGossiping && sender.equals(gossipTarget)) {
			this.receivedValue = otherVal;
			this.receiveStatus = true;
			this.mux.unlock();
			
		} else if(!isGossiping){
			this.mux.unlock();
			FollowGossip(sender, otherVal);
		}
		
		TryUpdateGossip();
	}

	@Override
	public void OnMessageStatus(String target, MessageStatus status) {
		this.mux.lock();
		
		updateGossiping();
		
		if(isGossiping && target == gossipTarget) {
			sendStatus = status;			
		}
		
		this.mux.unlock();

		TryUpdateGossip();
	}
	
}
