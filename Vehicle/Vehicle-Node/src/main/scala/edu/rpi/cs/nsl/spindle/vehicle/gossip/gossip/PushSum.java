package edu.rpi.cs.nsl.spindle.vehicle.gossip.gossip;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossip;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.messages.ValueWeightMessageData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PushSum implements IGossip {
    Logger logger = LoggerFactory.getLogger(this.getClass());
    protected double m_weight;
    protected double m_value;

    protected double m_tempWeight;
    protected double m_tempValue;

    public PushSum(double value, double weight) {
        //m_data = new PushSumData(value, weight);
        m_value = value;
        m_weight = weight;
    }

    @Override
    public IGossipMessageData GetLeadGossipMessage() {
        return GetGossipMessage();
    }

    @Override
    public IGossipMessageData GetGossipMessage() {
        m_tempValue = m_tempValue / 2.0;
        m_tempWeight = m_tempWeight / 2.0;

        ValueWeightMessageData messageData = new ValueWeightMessageData(m_tempValue, m_tempWeight);
        logger.debug("sending message ({},\t{})", messageData.getValue(), messageData.getWeight());

        return messageData;
    }

    @Override
    public boolean HandleUpdateMessage(String sender, Object message) {
        if (message instanceof ValueWeightMessageData) {
            ValueWeightMessageData castMessage = (ValueWeightMessageData) message;
            PushSumData newValue = new PushSumData(castMessage.getValue(), castMessage.getWeight());

            m_tempValue = m_tempValue + newValue.value;
            m_tempWeight = m_tempWeight + newValue.weight;

            return true;
        }

        logger.debug("bad message, don't know how to decode {}", message);
        return false;
    }

    @Override
    public void Abort() {
        m_tempWeight = m_weight;
        m_tempValue = m_value;
    }

    @Override
    public void Commit() {
        m_weight = m_tempWeight;
        m_value = m_tempValue;
    }

    @Override
    public Object GetValue() {
        //return m_data.value / m_data.weight;
        logger.debug("FINAL: {}\t{}", m_value/m_weight, m_weight);
        return m_value / m_weight;
    }
}
