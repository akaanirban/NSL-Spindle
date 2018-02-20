package edu.rpi.cs.nsl.spindle.vehicle.gossip.protocol;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.MessageStatus;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.messages.ConsensusFollowResponse;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.messages.ConsensusLeadGossipMessage;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.util.MessageQueueData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

public class ConsensusProtocol extends BaseProtocol {
    protected List<MessageQueueData> m_statusQueue;
    protected String m_id;
    private Logger logger = LoggerFactory.getLogger(this.getClass());


    protected boolean isLeading;
    protected boolean isFollowing;

    protected String target;

    public ConsensusProtocol(String id){

        m_statusQueue = new LinkedList<>();
        this.m_id = id;

        isLeading = false;
        isFollowing = false;
    }

    @Override
    public void doIteration() {
        m_messageQueueLock.lock();

        boolean notGossiping = !isLeading && !isFollowing;
        if(notGossiping) {
            // can process messages off the queue
            processNotGossiping();
        }
        else if(isFollowing) {
            processFollowing();
        }

        m_messageQueueLock.unlock();
    }

    protected void processFollowing() {
        // look for good status message in the queue
        // shouldn't have any stales...
        if(m_statusQueue.isEmpty()) {
            return;
        }

        MessageQueueData statusQueueData = m_statusQueue.remove(0);

        if(statusQueueData.Sender.equalsIgnoreCase(target)){
            MessageStatus status = (MessageStatus) statusQueueData.Message;
            if(status == MessageStatus.GOOD){
                m_gossip.Commit();
                logger.debug("following: good status, committing");
                isFollowing = false;
            }
            else {
                m_gossip.Abort();
                logger.debug("following: bad status, aborting");
                // TODO: should we try again?
                isFollowing = false;
            }
        }
        else {
            logger.debug("following: discarding status {} to {}",statusQueueData.Message, statusQueueData.Sender);
        }
    }

    protected void processNotGossiping() {
        if(m_messageQueue.isEmpty()) {
           return;
        }

        // else pull messages off the queue
        MessageQueueData messageQueueData = m_messageQueue.remove(0);
        if(messageQueueData.Message instanceof ConsensusLeadGossipMessage) {
            // good to follow, grab response and return
            ConsensusLeadGossipMessage message = (ConsensusLeadGossipMessage) messageQueueData.Message;
            m_gossip.HandleUpdateMessage(messageQueueData.Sender, message.getData());

            // build the response
            IGossipMessageData responseData = m_gossip.GetGossipMessage();
            ConsensusFollowResponse response = new ConsensusFollowResponse(responseData);

            // send the response
            m_networkSender.Send(messageQueueData.Sender, response);

            // say we are gossiping
            isFollowing = true;
            target = messageQueueData.Sender;
            logger.debug("sending message {} to {}", responseData, target);
        }
        else {
            // if its any other kind of message we should be able to discard it...
            logger.debug("discarding message {} from {}",messageQueueData.Message, messageQueueData.Sender);
        }
    }

    @Override
    public void OnMessageStatus(String target, MessageStatus status) {
        // TODO: add to queue with locking
        m_messageQueueLock.lock();
        m_statusQueue.add(new MessageQueueData(target, status));
        m_messageQueueLock.unlock();
    }

    @Override
    public void run() {
        // TODO: pull out and put in the base
    }
}
