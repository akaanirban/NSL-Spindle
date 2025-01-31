package edu.rpi.cs.nsl.spindle.vehicle.gossip.protocol.consensus;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.protocol.BaseProtocol;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.protocol.consensus.messages.ConsensusFollowResponse;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.protocol.consensus.messages.ConsensusLeadGossipMessage;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.protocol.consensus.messages.ConsensusNoGossipResponse;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.util.MessageQueueData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.util.MessageStatus;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.util.StatusQueueData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Protocol representing the "consensus gossip" protocol:
 * g_i(t+1) = f(g_i(t), g_j(t))
 * g_j(t+1) = f(g_i(t), g_j(t))
 * <p>
 * Tries to establish a "gossip session" by sending a lead message to target.
 * A "gossip session" is tracked with the UUID of the lead message.
 * <p>
 * If the lead message was sent properly, will parse messages:
 * message from any node other than "target":  send nogossip
 * from target follow: update gossip and commit
 * from target nogossip: abort
 * from target lead: send nogossip
 * <p>
 * If the protocol is not leading and receives a lead message, it will send a follow message.
 * If the follow message send is good, then it will commit.
 */
public class ConsensusProtocol extends BaseProtocol {
    protected String m_id;

    private Logger logger = LoggerFactory.getLogger(this.getClass());


    protected boolean isLeading;
    protected boolean isLeadingWaitingForStatus;
    protected boolean isLeadingWaitingForResponse;
    protected boolean isFollowing;

    protected UUID m_leaderMsgUUID;

    protected String m_target;
    protected UUID m_waitingStatusId; // uuid of message we are waiting for

    public ConsensusProtocol(String id) {
        super();

        this.m_id = id;

        isLeading = false;
        isLeadingWaitingForResponse = false;
        isLeadingWaitingForStatus = false;
        isFollowing = false;
    }

    @Override
    public void LeadGossip() {
        logger.debug("requesting lead gossip, mid: {} tar: {} msgId: {} leading: {} lwr: {} lws: {} follow: {}", m_id, m_target, m_leaderMsgUUID, isLeading, isLeadingWaitingForResponse, isLeadingWaitingForStatus, isFollowing);
        m_wantsLeadGossip.lazySet(true);
    }

    @Override
    public void DoIteration() {
        if (isFollowing) {
            ProcessFollowing();
        }
        else if (isLeading && isLeadingWaitingForStatus) {
            ProcessLeadingWaitStatus();
        }
        else if (isLeading && isLeadingWaitingForResponse) {
            ProcessLeadingWaitResponse();
        }
        else {
            // will handle starting the leadership
            ProcessNotGossiping();
        }
    }

    /**
     * wait for our specific status message in the queue
     *
     * @param whichStatus
     * @return MessageStatus.WAITING if didn't see it, otherwise returns the status
     */
    protected MessageStatus CheckForMessageStatus(UUID whichStatus) {

        if (IsStatusQueueEmptyThreadsafe()) {
            return MessageStatus.WAITING;
        }

        StatusQueueData statusQueueData = PopStatusQueueThreadsafe();

        if (statusQueueData.GetMessageId().equals(whichStatus)) {
            MessageStatus status = (MessageStatus) statusQueueData.GetMessage();
            logger.debug("got status {} for message {}", status, statusQueueData.GetMessageId());
            return status;
        }
        else {
            logger.debug("discarding status {} for message {}", statusQueueData.GetMessage(), statusQueueData.GetMessageId());
            return MessageStatus.WAITING;
        }
    }

    protected void ProcessFollowing() {
        // check if our message status is in the queue
        MessageStatus status = CheckForMessageStatus(m_waitingStatusId);
        if (status == MessageStatus.GOOD) {
            m_gossip.Commit();
            logger.debug("following: good status, committing");
            isFollowing = false;
        }
        else if (status == MessageStatus.BAD) {
            m_gossip.Abort();
            logger.debug("following: bad status, aborting");
            isFollowing = false;
        }
    }

    protected void ProcessLeadingWaitStatus() {
        MessageStatus status = CheckForMessageStatus(m_waitingStatusId);
        if (status == MessageStatus.GOOD) {
            logger.debug("leading: good status, waiting for response");

            // message sent, now need to wait for response
            isLeadingWaitingForStatus = false;
            isLeadingWaitingForResponse = true;
        }
        else if (status == MessageStatus.BAD) {
            m_gossip.Abort();

            logger.debug("leading: bad status, aborting");
            isLeading = false;
            isLeadingWaitingForResponse = false;
        }
    }

    protected void ProcessLeadingWaitResponse() {
        // leading, process messages as they come in
        if (IsMessageQueueEmptyThreadsafe()) {
            return;
        }

        // else pull messages off the queue
        MessageQueueData messageQueueData = PopMessageQueueThreadsafe();

        if (!messageQueueData.Sender.equalsIgnoreCase(m_target)) {
            // don't gossip with people other than our partner
            if (messageQueueData.Message instanceof ConsensusLeadGossipMessage) {
                ConsensusLeadGossipMessage message = (ConsensusLeadGossipMessage) messageQueueData.Message;
                ConsensusNoGossipResponse response = new ConsensusNoGossipResponse(message.GetUUID());
                m_networkSender.Send(messageQueueData.Sender, response);

                logger.debug("sending nogossip message to {}, waiting for {}", messageQueueData.Sender, m_target);
            }
            else {
                logger.debug("leading: discarding message {} from {}", messageQueueData.Message, messageQueueData.Sender);
            }

            return;
        }

        // know message is from our target, parse it
        if (messageQueueData.Message instanceof ConsensusFollowResponse) {
            ConsensusFollowResponse message = (ConsensusFollowResponse) messageQueueData.Message;

            // check that the response is good
            if (!message.GetLeadUUID().equals(m_leaderMsgUUID)) {
                // this should be a loud error
                logger.debug("ERROR: trying to lead {} but got follow {}", m_leaderMsgUUID, message);
                return;
            }

            logger.debug("leading: received message {} from {}, committing!", message, messageQueueData.Sender);

            m_gossip.HandleUpdateMessage(messageQueueData.Sender, message.GetData());
            m_gossip.Commit();

            // done gossiping
            isLeading = false;
            isLeadingWaitingForResponse = false;

        }
        else if (messageQueueData.Message instanceof ConsensusLeadGossipMessage) {
            // got a lead message from our partner. Send a nogossip message in response. They should do the same.
            // can't treat this like receiving a follow message because we want to be strict about "gossip session".
            logger.debug("leading: received leader message {} from {} while trying to gossip, sending nogossip", messageQueueData.Message, messageQueueData.Sender);

            // get the nogossip message
            ConsensusLeadGossipMessage message = (ConsensusLeadGossipMessage) messageQueueData.Message;

            // send no gossip message
            ConsensusNoGossipResponse response = new ConsensusNoGossipResponse(message.GetUUID());
            m_networkSender.Send(messageQueueData.Sender, response);

        }
        else if (messageQueueData.Message instanceof ConsensusNoGossipResponse) {
            // if we get a nogossip about this "gossip session" then we should break
            ConsensusNoGossipResponse message = (ConsensusNoGossipResponse) messageQueueData.Message;
            if (!message.GetLeadUUID().equals(m_leaderMsgUUID)) {
                // we shouldn't ever get a stale nogossip
                logger.debug("ERROR: trying to lead {} but got nogossip {}", m_leaderMsgUUID, message);
                return;
            }

            // stop leading and reset
            m_gossip.Abort();

            isLeading = false;
            isLeadingWaitingForResponse = false;

            logger.debug("leading: got nogossip message {} from {}", message, messageQueueData.Sender);

        }
        else {
            // if its any other kind of message we should be able to discard it...
            logger.debug("leading: discarding message {} from {}", messageQueueData.Message, messageQueueData.Sender);
        }
    }

    protected void ProcessNotGossiping() {
        if (IsMessageQueueEmptyThreadsafe()) {
            // unlock because definitely not continuing
            if (m_wantsLeadGossip.get()) {
                m_wantsLeadGossip.set(false);

                // choose a m_target, send the message
                String target = m_logicalNetwork.ChooseRandomTarget();
                if (m_id.equalsIgnoreCase(target)) {
                    logger.debug("{} wants to target {}, ignoring", m_id, target);
                    return;
                }

                logger.debug("{} wants to target {}, allowing", m_id, target);

                IGossipMessageData data = m_gossip.GetLeadGossipMessage();
                ConsensusLeadGossipMessage message = new ConsensusLeadGossipMessage(data);

                m_networkSender.Send(target, message);

                // now set the state
                m_target = target;

                // which message we want to wait for
                m_waitingStatusId = message.GetUUID();
                m_leaderMsgUUID = message.GetUUID();

                isLeading = true;
                isLeadingWaitingForStatus = true;
                isLeadingWaitingForResponse = false;
            }

            return;
        }

        // else pull messages off the queue
        MessageQueueData messageQueueData = PopMessageQueueThreadsafe();

        if (messageQueueData.Message instanceof ConsensusLeadGossipMessage) {
            // good to follow, grab response and return
            ConsensusLeadGossipMessage message = (ConsensusLeadGossipMessage) messageQueueData.Message;
            m_gossip.HandleUpdateMessage(messageQueueData.Sender, message.GetData());

            m_leaderMsgUUID = message.GetUUID();

            // build the response
            IGossipMessageData responseData = m_gossip.GetGossipMessage();
            ConsensusFollowResponse response = new ConsensusFollowResponse(responseData, m_leaderMsgUUID);

            // send the response
            m_networkSender.Send(messageQueueData.Sender, response);

            // say we are gossiping
            isFollowing = true;
            m_target = messageQueueData.Sender;
            m_waitingStatusId = response.GetUUID();

            logger.debug("sending message {} to {}", responseData, m_target);
        }
        else {
            // if its any other kind of message we should be able to discard it...
            logger.debug("discarding message {} from {}", messageQueueData.Message, messageQueueData.Sender);
        }
    }

    @Override
    public void run() {
        // TODO: pull out and put in the base
        logger.debug("starting consensus protocol");

        while (true) {
            if (m_wantsStop.get() == true) {
                logger.debug("stopping");
                break;
            }

            DoIteration();

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                logger.error("ERROR, failed to sleep properly", e);
            }
        }
    }
}
