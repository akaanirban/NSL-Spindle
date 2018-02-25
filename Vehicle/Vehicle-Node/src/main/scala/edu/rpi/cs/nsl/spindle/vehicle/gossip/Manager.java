package edu.rpi.cs.nsl.spindle.vehicle.gossip;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossip;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipProtocol;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.network.ConnectionMap;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.network.NetworkLayer;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.query.Query;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.query.QueryBuilder;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.query.QueryRouter;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.util.ProtocolScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Contains the logic for managing gossip running.
 * Handles queries, epochs
 */
public class Manager {
    Logger logger = LoggerFactory.getLogger(this.getClass());


    protected Map<Query, IGossipProtocol> m_protocols;
    protected Map<Query, Thread> m_protocolThreads;

    protected Map<Query, ProtocolScheduler> m_schedulers;
    protected Map<Query, Thread> m_schedulerThreads;

    protected Set<Query> m_queries;

    protected QueryBuilder m_queryBuilder;
    protected QueryRouter m_router;

    protected ConnectionMap m_connectionMap;
    protected NetworkLayer m_networkLayer;

    public Manager(QueryBuilder builder, ConnectionMap connectionMap, NetworkLayer networkLayer) {
        m_protocols = new TreeMap<>();
        m_protocolThreads = new TreeMap<>();

        m_schedulers = new TreeMap<>();
        m_schedulerThreads = new TreeMap<>();

        m_queries = new TreeSet<>();
        m_queryBuilder = builder;

        m_connectionMap = connectionMap;
        m_networkLayer = networkLayer;
    }

    public Map<Query, Object> GetResults() {
        Map<Query, Object> result = new TreeMap<>();
        for(Map.Entry<Query, IGossipProtocol> entry : m_protocols.entrySet()) {
            IGossip gossip = entry.getValue().GetGossip();
            Object value = gossip.GetValue();

            result.put(entry.getKey(), value);
        }

        return result;
    }

    public void Start() {
        StartNewRound();
    }

    public void Stop() {
        logger.debug("stop called");
        StopProtocols();
        logger.debug("stop done");
    }

    // adds query to the list so that next itr it will be created
    public void AddQuery(Query query) {
        if(m_queries.contains(query)) {
            logger.debug("set already contains: {}", query);
        }
        else {
            m_queries.add(query);
        }
    }

    public void StopSchedulers() {
        for(Map.Entry<Query, ProtocolScheduler> entry : m_schedulers.entrySet()) {
            ProtocolScheduler scheduler = entry.getValue();
            scheduler.Finish();
            logger.debug("asked to stop protocol: {}", entry.getKey());
        }

        // make sure everything shut down
        for(Map.Entry<Query, Thread> entry : m_schedulerThreads.entrySet()) {
            Thread thread = entry.getValue();
            try {
                thread.join();
                logger.debug("joined scheduler for {}", entry.getKey());
            } catch (InterruptedException e) {
                logger.debug("failed to join scheduler for {}: {}", entry.getKey(), e.getMessage());
            }
        }

        logger.debug("done joining schedulers");

        // now we can clear the list, no reference so should get GC'd
        // TODO: test that everything gets cleaned up properly
        m_schedulers.clear();
        m_schedulerThreads.clear();
    }

    protected void StopProtocols() {
        for(Map.Entry<Query, IGossipProtocol> entry : m_protocols.entrySet()) {
            IGossipProtocol protocol = entry.getValue();
            protocol.Stop();
        }

        // now check the futures to make sure everything shut down
        for(Map.Entry<Query, Thread> entry : m_protocolThreads.entrySet()) {
            Thread thread = entry.getValue();
            try {
                thread.join();
                logger.debug("joined protocol for {}", entry.getKey());
            } catch (InterruptedException e) {
                logger.debug("failed to join protocol for {}: {}", entry.getKey(), e.getMessage());
            }
        }

        logger.debug("done joining protocols");

        // now we can clear the list, no reference so should get GC'd
        // TODO: test that everything gets cleaned up properly
        m_protocols.clear();
        m_protocolThreads.clear();
    }

    protected void StartNewRound() {
        StopProtocols();

        // wire everything up, the order needs to be:
        // 1) connect the protocols to the router
        // 2) connect the router to the network
        // 3) start the protocol / protocol scheduler threads
        //
        // the router needs to have all the protocols before it can connect to the network. If the network gets a
        // message for q2, but we haven't added the protocol for q2 to the router yet, then we will erroneously discard
        // the message.
        //
        // If the protocol send a message before it is observing the network then the status could get lost and it
        // may "hang". We could either buffer the status or just start the threads after we are all wired up.

        m_router = new QueryRouter();

        // now for each query, build it and insert it
        for(Query query : m_queries){
            logger.debug("building protocol for {}", query);
            // has the gossip but nothing else
            IGossipProtocol protocol = m_queryBuilder.BuildGossipProtocolFor(query);

            // wire the protocol to the router
            protocol.SetConnectionMap(m_connectionMap);
            m_router.InsertOrReplace(query, protocol);

            // build the threads but don't start them until everything is wired up
            Thread protocolThread = new Thread(protocol);

            ProtocolScheduler scheduler = new ProtocolScheduler(protocol, 800);
            Thread schedulerThread = new Thread(scheduler);

            logger.debug("storing");
            try {
                logger.debug("storing 1");
                m_schedulers.put(query, scheduler);
                logger.debug("storing 2");
                m_schedulerThreads.put(query, schedulerThread);
                logger.debug("storing 3");
                m_protocols.put(query, protocol);
                logger.debug("storing 4");
                m_protocolThreads.put(query, protocolThread);
            }catch(Exception e) {
                logger.error("ERROR building: {}", e.getMessage());
            }

            logger.debug("done storing!");
        }

        // connect the router to the network
        m_router.SetNetwork(m_networkLayer);
        m_networkLayer.AddObserver(m_router);

        // now we can start the threads
        for(Query query : m_queries) {
            m_protocolThreads.get(query).start();
            m_schedulerThreads.get(query).start();
        }
        logger.debug("done starting new round");
    }
}
