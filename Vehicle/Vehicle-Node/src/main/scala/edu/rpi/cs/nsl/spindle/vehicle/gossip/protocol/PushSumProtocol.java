package edu.rpi.cs.nsl.spindle.vehicle.gossip.protocol;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.MessageStatus;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.util.MessageQueueData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class PushSumProtocol extends BaseProtocol {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private String m_id;

    public PushSumProtocol(String id) {
        super();
        this.m_id = id;
    }

    @Override
    public void OnMessageStatus(UUID messageId, MessageStatus status) {
        logger.debug("{} status {}", messageId, status);
    }

    @Override
    public void run() {
        logger.debug("starting pushsum protocol");
        while(true){
            if(m_wantsStop.get() == true){
                logger.debug("stopping");
                break;
            }

            doIteration();
        }
    }

    @Override
    public void doIteration() {
        // try to get messages out of the queue
        // if there are no messages to process, then check if we want to gossip
        m_messageQueueLock.lock();
        if(m_messageQueue.isEmpty() == false){
            logger.debug("pulling message from queue");
            MessageQueueData messageQueueData = m_messageQueue.remove(0);
            m_gossip.HandleUpdateMessage(messageQueueData.Sender, messageQueueData.Message);
            // TODO: wait for the message status
            m_gossip.Commit();
        }
        else if(m_wantsLeadGossip.get() == true){
            m_wantsLeadGossip.set(false);
            logger.debug("trying to lead gossip");
            List<String> targets = ChooseTargets();

            // don't bother sending a message to ourself
            if(!targets.get(0).equalsIgnoreCase(m_id)){
                IGossipMessageData toSend = m_gossip.GetLeadGossipMessage();
                m_networkSender.Send(targets.get(0), toSend);
                // TODO: wait for the message status
                m_gossip.Commit();
            }
            assert(m_wantsLeadGossip.get() == false);
        }

        m_messageQueueLock.unlock();
    }

    protected List<String> ChooseTargets() {
        ArrayList<String> targets = new ArrayList<>();
        targets.add(m_logicalNetwork.ChooseRandomTarget());

        return targets;
    }
}
