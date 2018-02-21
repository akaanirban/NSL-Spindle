package edu.rpi.cs.nsl.spindle.vehicle.gossip.query;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.MessageStatus;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.*;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.util.MessageQueueData;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The query router sits between the gossip protocol and the network, and makes sure that any messages
 * get to the proper place
 */
public class QueryRouter implements INetworkSender, INetworkObserver, Runnable {

    protected INetworkSender m_networkSender;

    protected Map<Query, IGossipProtocol> m_protocols;
    protected List<MessageQueueData> m_messageQueue;
    protected List<MessageQueueData> m_statusQueue;

    protected Lock m_lock;
    protected AtomicBoolean m_wantsStop;

    public void SetNetwork(INetworkSender sender) {
        m_networkSender = sender;
    }


    public QueryRouter() {
        m_messageQueue = new LinkedList<>();
        m_statusQueue = new LinkedList<>();

        m_lock = new ReentrantLock();
        m_wantsStop = new AtomicBoolean(false);
    }

    public void AddQuery(Query query) {
        // first check if we have an existing query
        // if not, add a new query
        // TODO: make this extensible... no reason the router should know how to parse queries
    }

    @Override
    public void Send(String target, IGossipMessageData message) {

    }

    public void doIteration() {

    }

    @Override
    public void run() {

    }

    @Override
    public void OnNetworkActivity(String sender, Object message) {

    }

    @Override
    public void OnMessageStatus(UUID messageId, MessageStatus status) {

    }
}
