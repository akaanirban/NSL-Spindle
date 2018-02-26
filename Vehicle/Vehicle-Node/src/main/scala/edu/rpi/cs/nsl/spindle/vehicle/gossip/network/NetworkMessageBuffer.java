package edu.rpi.cs.nsl.spindle.vehicle.gossip.network;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.MessageStatus;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkObserver;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.util.MessageQueueData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class NetworkMessageBuffer implements INetworkObserver {
    Logger logger = LoggerFactory.getLogger(this.getClass());

    protected List<MessageQueueData> m_messageBuffer;
    protected Lock m_messageQueueLock;

    protected INetworkObserver m_observer;
    protected AtomicBoolean m_hasObserver;

    public NetworkMessageBuffer() {
        m_messageBuffer = new ArrayList<>();
        m_messageQueueLock = new ReentrantLock();

        m_hasObserver = new AtomicBoolean(false);
    }

    public void SetObserver(INetworkObserver observer) {
        m_messageQueueLock.lock();

        this.m_observer = observer;
        // now lock and move all messages into observer's queue

        logger.debug("adding messages to queue, there are: {}", m_messageBuffer.size());
        for(MessageQueueData item : m_messageBuffer) {
            m_observer.OnNetworkActivity(item.Sender, item.Message);
        }

        m_hasObserver.set(true);
        m_messageQueueLock.unlock();
    }

    @Override
    public void OnNetworkActivity(String sender, Object message) {
        if(m_hasObserver.get()) {
            m_observer.OnNetworkActivity(sender, message);
            logger.debug("got message, have observer, sending up");
        }
        else {
            m_messageQueueLock.lock();
            logger.debug("no observer, adding message to buffer");
            m_messageBuffer.add(new MessageQueueData(sender, message));
            m_messageQueueLock.unlock();
        }
    }

    @Override
    public void OnMessageStatus(UUID messageId, MessageStatus status) {
        if(m_hasObserver.get()) {
            m_observer.OnMessageStatus(messageId, status);
        }
        else {
            logger.debug("ERROR: have status but no observer!");
        }
    }
}
