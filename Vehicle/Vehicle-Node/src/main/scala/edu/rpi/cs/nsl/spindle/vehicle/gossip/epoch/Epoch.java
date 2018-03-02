package edu.rpi.cs.nsl.spindle.vehicle.gossip.epoch;

import java.io.Serializable;
import java.time.Instant;

public class Epoch implements Serializable {

    protected Instant m_instant;

    public Epoch(Instant instant) {
        this.m_instant = instant;
    }

    public boolean IsSamePeriod(Epoch other) { return m_instant.equals(other.m_instant); }
    public boolean IsBefore(Epoch other) {
        return m_instant.isBefore(other.m_instant);
    }

    @Override
    public String toString() {
        return "[instant=" + m_instant.toString() + "]";
    }
}
