package edu.rpi.cs.nsl.spindle.vehicle.gossip.gossip;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossip;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.messages.ValueWeightMessageData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PushSum implements IGossip {
    Logger logger = LoggerFactory.getLogger(this.getClass());
    protected PushSumData m_data;
    protected double m_weight;
    protected double m_value;

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
        m_value = m_value / 2.0;
        m_weight = m_weight / 2.0;

        ValueWeightMessageData messageData = new ValueWeightMessageData(m_value, m_weight);
        logger.debug("sending message ({},\t{})", messageData.getValue(), messageData.getWeight());

        return messageData;
    }

    @Override
    public boolean HandleUpdateMessage(String sender, Object message) {
        if (message instanceof ValueWeightMessageData) {
            ValueWeightMessageData castMessage = (ValueWeightMessageData) message;
            PushSumData newValue = new PushSumData(castMessage.getValue(), castMessage.getWeight());

            logger.debug("estimated old weight: ({}\t{})", m_value, m_weight);
            m_value = m_value + newValue.value;
            m_weight = m_weight + newValue.weight;
            //m_data.Update(newValue);

            logger.debug("updated with ({},\t{})\tnew val: ({},\t{})\t, new estimate: {}",
                    newValue.value, newValue.weight, m_value, m_weight, m_value / m_weight);
            //        newValue.weight, newValue.value, m_data.value / m_data.weight);

            return true;
        }

        logger.debug("bad message, don't know how to decode {}", message);
        return false;
    }

    @Override
    public void Reset() {
        // TODO: implement
    }

    @Override
    public Object GetValue() {
        //return m_data.value / m_data.weight;
        logger.debug("FINAL: {}\t{}", m_value/m_weight, m_weight);
        return m_value / m_weight;
    }
}
