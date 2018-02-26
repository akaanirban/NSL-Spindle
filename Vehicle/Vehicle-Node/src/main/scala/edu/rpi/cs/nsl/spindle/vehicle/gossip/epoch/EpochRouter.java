package edu.rpi.cs.nsl.spindle.vehicle.gossip.epoch;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.MessageStatus;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkObserver;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkSender;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.messages.EpochTaggedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public EpochRouter(INetworkSender sender) {
        this.m_sender = sender;
        m_lock = new ReentrantLock();
    }

    public void SetObserver(INetworkObserver observer) {
        m_observer = observer;
    }

    public void SetEpoch(Epoch epoch) {
        m_lock.lock();
        m_currentEpoch = epoch;
        m_lock.unlock();
    }

    @Override
    public void OnNetworkActivity(String sender, Object message) {
        // try parse out the epoch. If it == ours, then we can send
        if(message instanceof EpochTaggedMessage) {
            EpochTaggedMessage taggedMessage = (EpochTaggedMessage) message;

            m_lock.lock();
            if(taggedMessage.GetEpoch().equals(m_currentEpoch)){
                // can send up the message
                m_lock.unlock();
                m_observer.OnNetworkActivity(sender, taggedMessage.getData());
            }
            else {
                m_lock.unlock();
                logger.debug("WARN: wrong epoch for message {}", taggedMessage);
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
