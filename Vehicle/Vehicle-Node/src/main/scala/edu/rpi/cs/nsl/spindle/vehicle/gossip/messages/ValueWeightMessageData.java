package edu.rpi.cs.nsl.spindle.vehicle.gossip.messages;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;

import java.util.UUID;

public class ValueWeightMessageData extends BaseMessage {

    private double m_value;
    private double m_weight;

    public ValueWeightMessageData(double value, double weight) {
        this.m_value = value;
        this.m_weight = weight;
    }

    @Override
    public Object getData() {
        return this;
    }

    public double getValue() {
        return m_value;
    }

    public double getWeight() {
        return m_weight;
    }

    @Override
    public String toString() {
        return "[id=" + m_uuid.toString() + "]";

    }
}
