package edu.rpi.cs.nsl.spindle.vehicle.gossip.network;

import java.io.Serializable;

/**
 * used to establish a socket and find the name
 */
public class StartUpMessage implements Serializable {
    public String sourceID;

    public StartUpMessage(String ID) {
        this.sourceID = ID;
    }

    @Override
    public String toString() {
        return "[type=startM sourceid=" + sourceID + "]";
    }
}
