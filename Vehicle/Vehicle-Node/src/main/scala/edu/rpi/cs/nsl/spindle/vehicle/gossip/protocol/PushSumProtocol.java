package edu.rpi.cs.nsl.spindle.vehicle.gossip.protocol;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.util.MessageQueueData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.util.MessageStatus;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.util.StatusQueueData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Protocol representing the "push-sum gossip" protocol:
 * g_i(t+1) = f(g_i(t), g_j(t) ...)
 * <p>
 * When it receives a message, it updates its current gossip state.
 * <p>
 * If want to lead, send a message to some target other than itself.
 */
public class PushSumProtocol extends BaseProtocol {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private String m_id;

    protected boolean m_isWaitingStatus;
    protected UUID m_waitingOnUUID;

    public PushSumProtocol(String id) {
        super();
        this.m_id = id;

        m_isWaitingStatus = false;
    }

    @Override
    public void run() {
        logger.debug("starting pushsum protocol");
        while (true) {
            if (m_wantsStop.get() == true) {
                logger.debug("stopping");
                break;
            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                logger.error("ERROR, failed to sleep properly", e);
            }

            DoIteration();
        }
    }

    @Override
    public void DoIteration() {
        // try to get messages out of the queue
        // if there are no messages to process, then check if we want to gossip
        if (m_isWaitingStatus) {
            logger.debug("processing waiting status");
            ProcessWaitingStatus();
        }
        else if (IsMessageQueueEmptyThreadsafe() == false) {
            ProcessMessages();
        }
        else if (m_wantsLeadGossip.get() == true) {
            ProcessLead();
        }
    }

    protected void ProcessWaitingStatus() {
        if (IsStatusQueueEmptyThreadsafe()) {
            return;
        }

        StatusQueueData statusQueueData = PopStatusQueueThreadsafe();
        if (statusQueueData.GetMessageId().equals(m_waitingOnUUID)) {
            if (statusQueueData.GetMessage() == MessageStatus.GOOD) {
                m_gossip.Commit();
                logger.debug("good status from message {}, committing", statusQueueData.GetMessageId());
            }
            else if (statusQueueData.GetMessage() == MessageStatus.BAD) {
                m_gossip.Abort();
                logger.debug("bad status from message {}, aborting", statusQueueData.GetMessageId());
            }

            // no longer waiting status
            m_isWaitingStatus = false;

        }
        else {
            logger.debug("discaring message {} with status {}", statusQueueData.GetMessageId(), statusQueueData.GetMessage());
        }
    }

    protected void ProcessMessages() {
        logger.debug("pulling message from queue");
        MessageQueueData messageQueueData = PopMessageQueueThreadsafe();
        m_gossip.HandleUpdateMessage(messageQueueData.Sender, messageQueueData.Message);

        // can always commit if we got it
        m_gossip.Commit();
    }

    protected void ProcessLead() {
        m_wantsLeadGossip.set(false);
        logger.debug("trying to lead gossip");
        List<String> targets = ChooseTargets();

        // don't bother sending a message to ourself
        if (!targets.get(0).equalsIgnoreCase(m_id)) {
            IGossipMessageData toSend = m_gossip.GetLeadGossipMessage();
            m_networkSender.Send(targets.get(0), toSend);

            m_isWaitingStatus = true;
            m_waitingOnUUID = toSend.GetUUID();
        }
    }

    protected List<String> ChooseTargets() {
        ArrayList<String> targets = new ArrayList<>();
        targets.add(m_logicalNetwork.ChooseRandomTarget());

        return targets;
    }
}
