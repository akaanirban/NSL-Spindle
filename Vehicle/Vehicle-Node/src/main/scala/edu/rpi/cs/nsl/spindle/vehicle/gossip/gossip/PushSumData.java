package edu.rpi.cs.nsl.spindle.vehicle.gossip.gossip;

import java.io.Serializable;

public class PushSumData implements Serializable {
	@Override
	public String toString() {
		return "PushSumData [value=" + value + "]";
	}

	public double value;
	public double weight;
	
	public PushSumData(Double value, Double weight) {
		this.value = value;
		this.weight = weight;
	}
	
	public PushSumData GetHalf() {
		return new PushSumData(value / 2.0, weight / 2.0);
	}
	
	public void Update(PushSumData other) {
		value = value + other.value;
		weight = weight + other.weight;
	}
}
