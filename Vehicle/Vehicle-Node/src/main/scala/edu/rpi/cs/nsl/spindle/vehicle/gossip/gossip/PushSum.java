package edu.rpi.cs.nsl.spindle.vehicle.gossip.gossip;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PushSum extends NormalGossip {
	Logger logger = LoggerFactory.getLogger(this.getClass());
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

		logger.debug("trying to gossip with: " + target);
		this.mux.unlock();
	}

	public void FollowGossip(String target, PushSumData otherValue) {
		this.mux.lock();
		value.Update(otherValue);
		logger.debug("got gossip from: " + target + " with value: " + otherValue.value + "\t" + otherValue.weight);
		logger.debug("now have estimate: {} {}", GetValue(), this.value.weight);
		this.mux.unlock();
	}
}
