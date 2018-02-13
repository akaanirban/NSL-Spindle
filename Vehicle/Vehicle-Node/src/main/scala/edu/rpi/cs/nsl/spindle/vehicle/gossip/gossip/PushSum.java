package edu.rpi.cs.nsl.spindle.vehicle.gossip.gossip;

public class PushSum extends NormalGossip {
	public PushSum() {
		super();
		this.value = new PushSumData(0.0, 1.0);
	}
	
	public double GetValue() {
		return value.value / value.weight;
	}

	protected void TryUpdateGossip() {

	}

	public void LeadGossip(String target) {
		this.mux.lock();
		value = value.GetHalf();
		sender.Send(target, value);

		this.mux.unlock();
	}

	public void FollowGossip(String target, PushSumData otherValue) {
		this.mux.lock();
		value.Update(otherValue);
		this.mux.unlock();
	}
}
