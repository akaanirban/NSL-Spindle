package edu.rpi.cs.nsl.spindle.vehicle.gossip;

import java.io.Serializable;

public class StartUpMessage implements Serializable{
	public String sourceID;
	
	public StartUpMessage(String ID) {
		this.sourceID = ID;
	}
}
