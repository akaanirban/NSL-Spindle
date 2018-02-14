package edu.rpi.cs.nsl.spindle.vehicle.gossip.protocol;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossip;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipProtocol;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkSender;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.network.ConnectionMap;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.util.MessageQueueData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * contains some useful base helper methods
 */
public abstract class BaseProtocol implements IGossipProtocol {
    Logger logger = LoggerFactory.getLogger(this.getClass());

    protected IGossip m_gossip;
    protected INetworkSender m_networkSender;
    protected ConnectionMap m_connectionMap;

    protected AtomicBoolean m_wantsLeadGossip;
    protected AtomicBoolean m_wantsStop;

    protected List<MessageQueueData> m_messageQueue;
    protected Lock m_messageQueueLock;

    public BaseProtocol() {
        m_wantsLeadGossip = new AtomicBoolean(false);
        m_wantsStop = new AtomicBoolean(false);

        m_messageQueue = new LinkedList<>();

        // TODO: double check that we are OK to use this mechanism
        m_messageQueueLock = new ReentrantLock();
    }

    @Override
    public void SetGossip(IGossip gossip) {
        m_gossip = gossip;
    }

    @Override
    public IGossip GetGossip() {
        return m_gossip;
    }

    @Override
    public void SetNetwork(INetworkSender sender) {
        m_networkSender = sender;
    }

    @Override
    public void SetConnectionMap(ConnectionMap connectionMap) {
        m_connectionMap = connectionMap;
    }

    @Override
    public void LeadGossip() {
        logger.debug("requesting lead gossip");
        m_wantsLeadGossip.lazySet(true);
    }

    @Override
    public void OnNetworkActivity(String sender, Object message) {
        m_messageQueueLock.lock();
        logger.debug("from {} queueing {}",sender, message);
        m_messageQueue.add(new MessageQueueData(sender, message));
        m_messageQueueLock.unlock();
    }

    @Override
    public void Stop() {
        m_wantsStop.lazySet(true);
    }
}
