package edu.rpi.cs.nsl.spindle.vehicle.gossip.epoch;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.MessageStatus;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkObserver;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkSender;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.messages.EpochTaggedMessage;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.network.NetworkMessageBuffer;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.util.MessageQueueData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Filters out all receive messages that are "old"
 */
public class EpochRouter implements INetworkObserver, INetworkSender{
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

    public void StartBuffering() {
        m_lock.lock();
        m_isBuffering = true;
        m_lock.unlock();
    }

    public void SetEpoch(Epoch epoch) {
        m_lock.lock();
        m_currentEpoch = epoch;
        m_isBuffering = false;

        // on epoch ending, need to clear the buffer and send everything from this epoch up
        List<MessageQueueData> m_bufferCopy = new LinkedList<>(m_buffer);
        m_buffer.clear();

        for(MessageQueueData messageQueueData : m_bufferCopy) {
            OnNetworkActivity(messageQueueData.Sender, messageQueueData.Message);
        }

        m_lock.unlock();
    }

    @Override
    public void OnNetworkActivity(String sender, Object message) {
        // try parse out the epoch. If it == ours, then we can send
        if(message instanceof EpochTaggedMessage) {
            EpochTaggedMessage taggedMessage = (EpochTaggedMessage) message;

            m_lock.lock();

            // if we are buffering, then add it to the buffer, otherwise try to parse it out
            if(m_isBuffering) {
                m_buffer.add(new MessageQueueData(sender, message));
                m_lock.unlock();
                return;
            }

            Epoch epoch = taggedMessage.GetEpoch();
            if(m_currentEpoch.IsSamePeriod(epoch)){
                // can send up the message
                m_lock.unlock();
                m_observer.OnNetworkActivity(sender, taggedMessage.getData());
            }
            else if(m_currentEpoch.IsBefore(epoch)) {
                // message from the future!
                // buffer it in the future epochs buffer
                m_buffer.add(new MessageQueueData(sender, message));
                logger.debug("buffering message {} from future epoch {}", message, epoch);
                m_lock.unlock();
            }
            else {
                m_lock.unlock();
                logger.debug("WARN: wrong epoch for message {} current is {}, got {}",
                        taggedMessage, m_currentEpoch, epoch);
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
