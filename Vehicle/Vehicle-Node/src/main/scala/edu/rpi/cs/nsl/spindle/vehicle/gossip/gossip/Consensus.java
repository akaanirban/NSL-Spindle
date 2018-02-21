package edu.rpi.cs.nsl.spindle.vehicle.gossip.gossip;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossip;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.messages.ValueWeightMessageData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consensus implements IGossip {
    Logger logger = LoggerFactory.getLogger(this.getClass());

    protected double m_value;
    protected double m_weight;

    protected double m_otherValue;
    protected double m_otherWeight;

    public Consensus(double value, double weight) {
        m_value = value;
        m_weight = weight;
    }

    @Override
    public IGossipMessageData GetLeadGossipMessage() {
        return GetGossipMessage();
    }

    @Override
    public IGossipMessageData GetGossipMessage() {
        double value = m_value;
        double weight = m_weight;

        ValueWeightMessageData messageData = new ValueWeightMessageData(value, weight);

        return messageData;
    }

    @Override
    public boolean HandleUpdateMessage(String sender, Object message) {
        if (message instanceof ValueWeightMessageData) {
            ValueWeightMessageData castMessage = (ValueWeightMessageData) message;

            m_otherValue = castMessage.getValue();
            m_otherWeight = castMessage.getWeight();

            return true;
        }

        logger.debug("bad message, don't know how to decode {}", message);
        return false;
    }

    @Override
    public void Abort() {
        m_otherValue = m_value;
        m_otherWeight = m_weight;
    }

    @Override
    public void Commit() {
        m_value = (m_otherValue + m_value) / 2.0;
        m_weight = (m_otherWeight + m_weight) / 2.0;
    }

    @Override
    public Object GetValue() {
        logger.debug("FINAL: {}\t{}", m_value/m_weight, m_weight);
        return m_value / m_weight;
    }
}
