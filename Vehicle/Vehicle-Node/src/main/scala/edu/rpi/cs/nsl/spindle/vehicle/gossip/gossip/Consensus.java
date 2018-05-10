package edu.rpi.cs.nsl.spindle.vehicle.gossip.gossip;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossip;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.messages.ValueWeightMessageData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * data object for consensus gossip
 * note that this contains some noisy debug info
 */
public class Consensus implements IGossip {
    Logger logger = LoggerFactory.getLogger(this.getClass());

    protected double m_value;
    protected double m_weight;

    protected double m_otherValue;
    protected double m_otherWeight;

    protected boolean isLeading;
    protected boolean isGossiping;

    protected UUID m_leadUUID;
    protected UUID m_followUUID;

    public Consensus(double value, double weight) {
        m_value = value;
        m_weight = weight;

        isLeading = false;
        isGossiping = false;
    }

    @Override
    public IGossipMessageData GetLeadGossipMessage() {
        double value = m_value;
        double weight = m_weight;

        ValueWeightMessageData messageData = new ValueWeightMessageData(value, weight);
        if (isLeading || isGossiping) {
            logger.debug("ERROR: already gossiping!");
        }

        isLeading = true;
        isGossiping = true;
        m_leadUUID = messageData.GetUUID();

        return messageData;
    }

    @Override
    public IGossipMessageData GetGossipMessage() {
        double value = m_value;
        double weight = m_weight;

        ValueWeightMessageData messageData = new ValueWeightMessageData(value, weight);
        if (isLeading) {
            logger.debug("ERROR: already leading!");
        }
        if (!isGossiping) {
            logger.debug("ERROR: should be gossiping!");
        }

        isGossiping = true;
        m_followUUID = messageData.GetUUID();

        return messageData;
    }

    @Override
    public boolean HandleUpdateMessage(String sender, Object message) {
        if (message instanceof ValueWeightMessageData) {
            ValueWeightMessageData castMessage = (ValueWeightMessageData) message;

            m_otherValue = castMessage.getValue();
            m_otherWeight = castMessage.getWeight();

            if (!isGossiping) {
                // not gossiping, therefore shouldn't be leading!
                if (isLeading) {
                    logger.debug("ERROR: should not be leading!");
                }
                m_leadUUID = castMessage.GetUUID();
            }
            else {
                // gossiping, so should be leading!
                m_followUUID = castMessage.GetUUID();
                if (!isLeading) {
                    logger.debug("ERROR: should be leading!");
                }
            }

            isGossiping = true;
            return true;
        }

        logger.debug("ERROR: bad message, don't know how to decode {}", message);
        return false;
    }

    @Override
    public void Abort() {
        m_otherValue = m_value;
        m_otherWeight = m_weight;

        if (!isGossiping) {
            logger.debug("ERROR: in abort should be gossiping");
        }
        isLeading = false;
        isGossiping = false;

        logger.debug("abort {} {}", m_leadUUID, m_followUUID);

        m_leadUUID = null;
        m_followUUID = null;
    }

    @Override
    public void Commit() {
        m_value = (m_otherValue + m_value) / 2.0;
        m_weight = (m_otherWeight + m_weight) / 2.0;

        isLeading = false;

        if (!isGossiping) {
            logger.debug("ERROR: in commit should be gossiping");
        }
        isLeading = false;
        isGossiping = false;

        logger.debug("commit {} {}", m_leadUUID, m_followUUID);

        m_leadUUID = null;
        m_followUUID = null;
    }

    @Override
    public Object GetValue() {
        double value = 0.0;
        if (m_weight > 0.0) {
            value = m_value / m_weight;
        }

        logger.debug("FINAL: {}\t{}", value, m_weight);
        return value;
    }
}
