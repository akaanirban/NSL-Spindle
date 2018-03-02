package edu.rpi.cs.nsl.spindle.vehicle.gossip.query;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.MessageStatus;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipProtocol;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkObserver;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkSender;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.messages.QueryTaggedMessage;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.util.MessageQueueData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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

    protected Map<UUID, Query> m_messageMap;

    public QueryRouter() {
        m_queryObservers = new TreeMap<>();
        m_messageQueue = new LinkedList<>();
        m_statusQueue = new LinkedList<>();

        m_messageMap = new HashMap<>();

        m_lock = new ReentrantLock();
    }

    public void SetNetwork(INetworkSender sender) {
        m_networkSender = sender;
    }

    /**
     * wires up the message
     *
     * @param query
     * @param observer
     */
    public void InsertOrReplace(Query query, IGossipProtocol observer) {
        if (m_queryObservers.containsKey(query)) {
            logger.debug("already contains: {}, replacing", query);

        }

        // add tagger and set the network
        QueryTagger tagger = new QueryTagger(query, this);
        observer.SetNetwork(tagger);

        m_queryObservers.put(query, observer);
    }

    @Override
    public void OnNetworkActivity(String sender, Object raw) {
        if (raw instanceof QueryTaggedMessage) {
            // pull out query, find the target, and send
            QueryTaggedMessage message = (QueryTaggedMessage) raw;
            Query query = message.GetQuery();

            m_lock.lock();
            if (m_queryObservers.containsKey(query)) {
                IGossipProtocol which = m_queryObservers.get(query);
                m_lock.unlock();
                which.OnNetworkActivity(sender, message.getData());
            } else {
                logger.error("ERROR: don't have query {} for message {}", query, message);
                m_lock.unlock();
            }
        } else {
            logger.error("unexpected message {} from {}", raw, sender);
        }
    }

    @Override
    public void OnMessageStatus(UUID messageId, MessageStatus status) {
        // figure out which query the msg is for, then send to that one
        m_lock.lock();
        if (!m_messageMap.containsKey(messageId)) {
            logger.error("ERROR: do not have message {}", messageId);
            m_lock.unlock();
            return;
        }

        Query query = m_messageMap.remove(messageId);
        IGossipProtocol which = m_queryObservers.get(query);

        logger.debug("passing up message status {} {}", messageId, status);
        m_lock.unlock();

        which.OnMessageStatus(messageId, status);
    }

    @Override
    public void Send(String target, IGossipMessageData raw) {
        if (raw instanceof QueryTaggedMessage) {
            // pull the uuid out so we can deal with regular messages
            QueryTaggedMessage message = (QueryTaggedMessage) raw;

            m_lock.lock();
            if (!m_queryObservers.containsKey(message.GetQuery())) {
                logger.error("ERROR: trying to send message {} with unknown query {}", message, message.GetQuery());
                m_lock.unlock();
            } else {
                logger.debug("inserting {}", message);
                m_messageMap.put(message.getUUID(), message.GetQuery());
                m_lock.unlock();

                m_networkSender.Send(target, raw);
                logger.debug("done with send {}", message);
            }
        } else {
            logger.error("ERROR: untagged message {}", raw);
        }
    }
}
