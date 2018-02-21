package edu.rpi.cs.nsl.spindle.vehicle.gossip.protocol;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.MessageStatus;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.messages.ConsensusFollowResponse;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.messages.ConsensusLeadGossipMessage;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.messages.ConsensusNoGossipResponse;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.util.MessageQueueData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.util.StatusQueueData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class ConsensusProtocol extends BaseProtocol {
    protected String m_id;

    private Logger logger = LoggerFactory.getLogger(this.getClass());


    protected boolean isLeading;
    protected boolean isLeadingWaitingForStatus;
    protected boolean isLeadingWaitingForResponse;
    protected boolean isFollowing;

    protected String m_target;
    protected UUID m_waitingStatusId; // uuid of message we are waiting for

    public ConsensusProtocol(String id){
        super();

        this.m_id = id;

        isLeading = false;
        isLeadingWaitingForResponse = false;
        isLeadingWaitingForStatus = false;
        isFollowing = false;
    }

    @Override
    public void doIteration() {
        m_messageQueueLock.lock();

        if(isFollowing) {
            processFollowing();
        }
        else if(isLeading && isLeadingWaitingForStatus) {
            processLeadingWaitStatus();
        }
        else if(isLeading && isLeadingWaitingForResponse) {
            processLeadingWaitRepsonse();
        }
        else {
            // will handle starting the leadership
            processNotGossiping();
        }

        m_messageQueueLock.unlock();
    }

    protected void processFollowing() {
        // look for good status message in the queue
        // shouldn't have any stales...
        if(m_statusQueue.isEmpty()) {
            return;
        }

        StatusQueueData statusQueueData = m_statusQueue.remove(0);

        if(statusQueueData.GetMessageId().equals(m_waitingStatusId)){
            MessageStatus status = (MessageStatus) statusQueueData.GetMessage();
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
            logger.debug("following: discarding status {}",
                    statusQueueData.GetMessage());
        }
    }

    protected void processLeadingWaitStatus() {
        if(m_statusQueue.isEmpty()){
            return;
        }

        // else pull message off
        StatusQueueData statusQueueData = m_statusQueue.remove(0);
        if(statusQueueData.GetMessageId().equals(m_waitingStatusId)){
            MessageStatus status = (MessageStatus) statusQueueData.GetMessage();
            if(status == MessageStatus.GOOD){
                logger.debug("leading: good status, waiting for response");

                // message sent, now need to wait for response
                isLeadingWaitingForStatus = false;
                isLeadingWaitingForResponse = true;
            }
            else {
                m_gossip.Abort();
                logger.debug("leading: bad status, aborting");
                // TODO: should we try again?
                isLeading = false;
                isLeadingWaitingForResponse = false;
            }
        }
        else {
            logger.debug("following: discarding status {}",
                    statusQueueData.GetMessage());
        }
    }

    protected void processLeadingWaitRepsonse() {
        if(m_messageQueue.isEmpty()){
            return;
        }

        // else pull messages off the queue
        MessageQueueData messageQueueData = m_messageQueue.remove(0);
        if(!messageQueueData.Sender.equalsIgnoreCase(m_target)) {
            // if someone sent us a lead message, ignore them
            if(messageQueueData.Message instanceof ConsensusLeadGossipMessage) {
                ConsensusNoGossipResponse response = new ConsensusNoGossipResponse();
                m_networkSender.Send(messageQueueData.Sender, response);

                logger.debug("sending nogossip message to {}", messageQueueData.Sender);
            }
            else {
                logger.debug("leading: discarding message {} from {}", messageQueueData.Message, messageQueueData.Sender);
            }

            return;
        }

        if(messageQueueData.Message instanceof ConsensusFollowResponse) {
            // got good response, can commit!
            ConsensusFollowResponse message = (ConsensusFollowResponse) messageQueueData.Message;
            m_gossip.HandleUpdateMessage(messageQueueData.Sender, message.getData());

            m_gossip.Commit();

            // done gossiping
            isLeading = false;
            isLeadingWaitingForResponse = false;

            logger.debug("leading: received message {} from {}, committing!", message, messageQueueData.Sender);
        }
        else if(messageQueueData.Message instanceof ConsensusLeadGossipMessage) {
            // if we get a lead msg from other, then we can treat it like a follow
            // TODO: verify this is OK
            ConsensusLeadGossipMessage message = (ConsensusLeadGossipMessage) messageQueueData.Message;
            m_gossip.HandleUpdateMessage(messageQueueData.Sender, message.getData());

            m_gossip.Commit();

            isLeading = false;
            isLeadingWaitingForResponse = false;
            logger.debug("leading: received message {} from {}, committing!", message, messageQueueData.Sender);
        }
        else if(messageQueueData.Message instanceof ConsensusNoGossipResponse) {
            // stop leading
            m_gossip.Abort();

            isLeading = false;
            isLeadingWaitingForResponse = false;

            logger.debug("leading: got nogossip message {} from {}",
                    messageQueueData.Sender, messageQueueData.Message);
        }
        else {
            // if its any other kind of message we should be able to discard it...
            logger.debug("leading: discarding message {} from {}",messageQueueData.Message, messageQueueData.Sender);
        }
    }

    protected void processNotGossiping() {
        if(m_messageQueue.isEmpty()) {
            if(m_wantsLeadGossip.get()) {
                // choose a m_target, send the message
                String target = m_logicalNetwork.ChooseRandomTarget();
                if(target.equalsIgnoreCase(m_id)) {
                    return;
                }

                IGossipMessageData data = m_gossip.GetLeadGossipMessage();
                ConsensusLeadGossipMessage message = new ConsensusLeadGossipMessage(data);

                m_networkSender.Send(target, message);

                // now set the state
                m_target = target;

                // which message we want to wait for
                m_waitingStatusId = message.getUUID();

                isLeading = true;
                isLeadingWaitingForStatus = true;
                isLeadingWaitingForResponse = false;
            }

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
            m_target = messageQueueData.Sender;
            m_waitingStatusId = response.getUUID();

            logger.debug("sending message {} to {}", responseData, m_target);
        }
        else {
            // if its any other kind of message we should be able to discard it...
            logger.debug("discarding message {} from {}",messageQueueData.Message, messageQueueData.Sender);
        }
    }

    @Override
    public void run() {
        // TODO: pull out and put in the base
        logger.debug("starting consensus protocol");

        while(true){
            if(m_wantsStop.get() == true){
                logger.debug("stopping");
                break;
            }

            doIteration();
        }
    }
}
