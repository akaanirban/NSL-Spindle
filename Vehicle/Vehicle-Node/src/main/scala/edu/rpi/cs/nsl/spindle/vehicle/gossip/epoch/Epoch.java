package edu.rpi.cs.nsl.spindle.vehicle.gossip.epoch;

import java.io.Serializable;
import java.time.Instant;

public class Epoch implements Serializable {

    protected Instant m_instant;

    public Epoch(Instant instant) {
        this.m_instant = instant;
    }
}
