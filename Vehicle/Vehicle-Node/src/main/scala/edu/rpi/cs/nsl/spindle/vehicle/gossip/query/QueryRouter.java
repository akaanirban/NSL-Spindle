package edu.rpi.cs.nsl.spindle.vehicle.gossip.query;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.MessageStatus;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.*;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.messages.QueryTaggedMessage;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.util.MessageQueueData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The query router sits between the gossip protocol and the network, and makes sure that any messages
 * get to the proper place
 */
public class QueryRouter implements INetworkObserver, INetworkSender {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    protected INetworkSender m_networkSender;

    protected Map<Query, IGossipProtocol> m_queryObservers;
    protected List<MessageQueueData> m_messageQueue;
    protected List<MessageQueueData> m_statusQueue;

    protected Lock m_lock;

    public void SetNetwork(INetworkSender sender) {
        m_networkSender = sender;
    }


    public QueryRouter() {
        m_queryObservers = new TreeMap<>();
        m_messageQueue = new LinkedList<>();
        m_statusQueue = new LinkedList<>();

        m_lock = new ReentrantLock();
    }

    /**
     * wires up the message
     * @param query
     * @param observer
     */
    public void InsertOrReplace(Query query, IGossipProtocol observer) {
        if(m_queryObservers.containsKey(query)) {
            logger.debug("already contains: {}, replacing", query);

        }

        // add tagger and set the network
        QueryTagger tagger = new QueryTagger(query, m_networkSender);
        observer.SetNetwork(tagger);

        m_queryObservers.put(query, observer);
    }

    @Override
    public void OnNetworkActivity(String sender, Object message) {
        if(message instanceof QueryTaggedMessage) {
            // pull out the query, send to the right place
        }
        else {
            logger.error("unexpected message {} from {}", message, sender);
        }
    }

    @Override
    public void OnMessageStatus(UUID messageId, MessageStatus status) {

    }

    @Override
    public void Send(String target, IGossipMessageData message) {

    }
}
