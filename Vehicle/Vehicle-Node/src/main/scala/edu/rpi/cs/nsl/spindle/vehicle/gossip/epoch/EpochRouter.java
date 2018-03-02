package edu.rpi.cs.nsl.spindle.vehicle.gossip.epoch;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.MessageStatus;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkObserver;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkSender;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.messages.EpochTaggedMessage;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.util.MessageQueueData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Filters messages that are not from the current epoch. Buffers messages from future epochs
 * Can act as a network buffer, storing all the messages it receives
 */
public class EpochRouter implements INetworkObserver, INetworkSender {
    Logger logger = LoggerFactory.getLogger(this.getClass());

    protected Epoch m_currentEpoch;
    protected INetworkSender m_sender;
    protected INetworkObserver m_observer;
    protected Lock m_lock;

    protected List<MessageQueueData> m_buffer;
    protected boolean m_isBuffering;

    public EpochRouter(INetworkSender sender) {
        this.m_sender = sender;

        m_isBuffering = true;
        m_buffer = new LinkedList<>();

        m_lock = new ReentrantLock();
    }

    public void SetObserver(INetworkObserver observer) {
        m_observer = observer;
    }

    /**
     * start buffering, stores all the messages received until SetEpcoh is called. Will still send messages
     */
    public void StartBuffering() {
        m_lock.lock();
        m_isBuffering = true;
        m_lock.unlock();
    }

    /**
     * set the new epoch and flush the buffer
     * NOTE: doesn't block once buffer gets cleared, so is safe to have upper layer that responds to it. However, the
     * messages, even from the same socket, may get delivered out of order
     *
     * @param epoch
     */
    public void SetEpoch(Epoch epoch) {
        m_lock.lock();
        m_currentEpoch = epoch;
        m_isBuffering = false;

        // on epoch ending, need to clear the buffer and send everything from this epoch up
        List<MessageQueueData> m_bufferCopy = new LinkedList<>(m_buffer);
        m_buffer.clear();

        m_lock.unlock();
        for (MessageQueueData messageQueueData : m_bufferCopy) {
            OnNetworkActivity(messageQueueData.Sender, messageQueueData.Message);
        }
    }

    @Override
    public void OnNetworkActivity(String sender, Object message) {
        // try parse out the epoch. If it == ours, then we can send
        if (message instanceof EpochTaggedMessage) {
            EpochTaggedMessage taggedMessage = (EpochTaggedMessage) message;

            // need to be really careful to avoid deadlock when sending up
            m_lock.lock();

            // if we are buffering, then add it to the buffer, otherwise try to parse it out
            if (m_isBuffering) {
                m_buffer.add(new MessageQueueData(sender, message));
                m_lock.unlock();
                return;
            }

            Epoch epoch = taggedMessage.GetEpoch();
            if (m_currentEpoch.IsSamePeriod(epoch)) {
                // can send up the message
                m_lock.unlock();
                m_observer.OnNetworkActivity(sender, taggedMessage.GetData());
            }
            else if (m_currentEpoch.IsBefore(epoch)) {
                // message from the future!
                // buffer it in the future epochs buffer
                m_buffer.add(new MessageQueueData(sender, message));
                logger.debug("buffering message {} from future epoch {}", message, epoch);
                m_lock.unlock();
            }
            else {
                logger.debug("WARN: wrong epoch for message {} current is {}, got {}", taggedMessage, m_currentEpoch, epoch);

                m_lock.unlock();
            }
        }
        else {
            logger.error("ERROR: could not parse message {}", message);
        }
    }

    @Override
    public void OnMessageStatus(UUID messageId, MessageStatus status) {
        m_observer.OnMessageStatus(messageId, status);
    }

    @Override
    public void Send(String target, IGossipMessageData message) {
        m_lock.lock();
        EpochTaggedMessage taggedMessage = new EpochTaggedMessage(message, m_currentEpoch);
        m_lock.unlock();

        m_sender.Send(target, taggedMessage);
    }
}
