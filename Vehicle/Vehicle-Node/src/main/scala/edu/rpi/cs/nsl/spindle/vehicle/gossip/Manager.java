package edu.rpi.cs.nsl.spindle.vehicle.gossip;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.epoch.Epoch;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.epoch.EpochRouter;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.epoch.IntervalHelper;
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

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Contains the logic for managing gossip running.
 * Handles queries, epochs
 */
public class Manager implements Runnable {
    Logger logger = LoggerFactory.getLogger(this.getClass());


    protected Map<Query, IGossipProtocol> m_protocols;
    protected Map<Query, Thread> m_protocolThreads;

    protected Map<Query, ProtocolScheduler> m_schedulers;
    protected Map<Query, Thread> m_schedulerThreads;

    protected Set<Query> m_queries;

    protected QueryBuilder m_queryBuilder;
    protected QueryRouter m_queryRouter;

    protected ConnectionMap m_connectionMap;
    protected NetworkLayer m_networkLayer;

    protected AtomicBoolean m_requestStop;
    protected IntervalHelper m_runScheduler;

    protected EpochRouter m_epochRouter;

    protected boolean m_isFirstRun;

    public Manager(QueryBuilder builder, ConnectionMap connectionMap, NetworkLayer networkLayer) {
        m_protocols = new TreeMap<>();
        m_protocolThreads = new TreeMap<>();

        m_schedulers = new TreeMap<>();
        m_schedulerThreads = new TreeMap<>();

        m_queries = new TreeSet<>();
        m_queryBuilder = builder;

        m_connectionMap = connectionMap;
        m_networkLayer = networkLayer;

        m_requestStop = new AtomicBoolean(false);
        m_runScheduler = new IntervalHelper(5);
        m_epochRouter = new EpochRouter(networkLayer);

        m_isFirstRun = true;
    }

    /**
     * gets result from all of the gossip protocols
     * NOTE: can't get result after the StopProtocol function called
     *
     * @return map[query, resultObj]
     */
    public Map<Query, Object> GetResults() {
        Map<Query, Object> result = new TreeMap<>();
        for (Map.Entry<Query, IGossipProtocol> entry : m_protocols.entrySet()) {
            IGossip gossip = entry.getValue().GetGossip();
            Object value = gossip.GetValue();

            result.put(entry.getKey(), value);
        }

        return result;
    }

    /**
     * Requests a stop
     */
    public void Stop() {
        m_requestStop.set(true);
    }

    /**
     * Add a query to the list of current queries
     *
     * @param query
     */
    public void AddQuery(Query query) {
        if (m_queries.contains(query)) {
            logger.debug("set already contains: {}", query);
        }
        else {
            m_queries.add(query);
        }
    }

    /**
     * hardstop the schedulers, blocks until they have all joined
     */
    public void StopSchedulers() {
        for (Map.Entry<Query, ProtocolScheduler> entry : m_schedulers.entrySet()) {
            ProtocolScheduler scheduler = entry.getValue();
            scheduler.Finish();
            logger.debug("asked to stop protocol: {}", entry.getKey());
        }

        // make sure everything shut down
        for (Map.Entry<Query, Thread> entry : m_schedulerThreads.entrySet()) {
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
        m_schedulers.clear();
        m_schedulerThreads.clear();
    }

    /**
     * hardstop the protocols, blocks until they have all joined.
     * NOTE: can't get the result after this method is called
     */
    protected void StopProtocols() {
        for (Map.Entry<Query, IGossipProtocol> entry : m_protocols.entrySet()) {
            IGossipProtocol protocol = entry.getValue();
            protocol.Stop();
        }

        // now check the futures to make sure everything shut down
        for (Map.Entry<Query, Thread> entry : m_protocolThreads.entrySet()) {
            Thread thread = entry.getValue();
            try {
                thread.join();
                logger.debug("joined protocol for {}", entry.getKey());
            } catch (InterruptedException e) {
                logger.debug("failed to join protocol for {}: {}", entry.getKey(), e.getMessage());
            }
        }

        logger.debug("done joining protocols");

        // now we can clear the list, should only be referenced by the scheduler
        m_protocols.clear();
        m_protocolThreads.clear();
    }

    public void StartNewRound() {
        // print the protocol results before killing them
        logger.debug("FINAL RESULT: {}", GetResults());

        // wire everything up, the order needs to be:
        // 1) set network as epoch sender (happens in constructor)
        // 2) start buffering the epoch router
        // 3) stop everything
        // 4) build query router
        // 5) build, but don't start protocols, can connect them to the query router
        // 6) let query router observe epoch, will get buffered messages
        // 7) stop buffering epoch
        // 8) if first run, let epoch router observe the network
        // 9) start scheduler, protocol threads
        //
        // General idea here is we need to connect layers in the opposite direction that messages flow so that if
        // messages start flowing we are definitely connected. On the observe side because we need to leave the network
        // layer up and running. The epoch router acts as a buffer, holding on to messages until the new epoch is set.
        //
        // Create a new query router and protocols each time so that we don't need to worry about resetting state.
        //
        // Only start threads once everything is hooked up. Any buffered messages should be connected.

        m_epochRouter.StartBuffering();

        // start buffering the epoch router
        Instant currentInstant = m_runScheduler.GetCurrentInterval();
        logger.debug("trying to start new round on epoch {}", currentInstant);

        // stop everything
        StopProtocols();
        StopSchedulers();

        // build the new query router
        m_queryRouter = new QueryRouter();

        // build and insert each query
        for (Query query : m_queries) {
            logger.debug("building protocol for {}", query);
            // has the gossip but nothing else
            IGossipProtocol protocol = m_queryBuilder.BuildGossipProtocolFor(query);

            // wire the protocol to the router
            protocol.SetConnectionMap(m_connectionMap);
            m_queryRouter.InsertOrReplace(query, protocol);

            // build the threads but don't start them until everything is wired up
            Thread protocolThread = new Thread(protocol);

            ProtocolScheduler scheduler = new ProtocolScheduler(protocol, 80);
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
            } catch (Exception e) {
                logger.error("ERROR building: {}", e.getMessage());
            }

            logger.debug("done storing!");
        }

        // connect the query router and the new epoch router
        m_queryRouter.SetNetwork(m_epochRouter);
        m_epochRouter.SetObserver(m_queryRouter);

        // this will send all the messages up to the query router
        m_epochRouter.SetEpoch(new Epoch(currentInstant));

        if (m_isFirstRun) {
            m_networkLayer.AddObserver(m_epochRouter);
            m_isFirstRun = false;
        }

        // now we can start the threads
        for (Query query : m_queries) {
            m_protocolThreads.get(query).start();
            m_schedulerThreads.get(query).start();
        }

        logger.debug("done starting new round");
    }

    @Override
    public void run() {
        Instant previous = m_runScheduler.GetNext();
        Timer timer = new Timer();

        while (!m_requestStop.get()) {
            Instant nextRunInstant = m_runScheduler.GetNext();

            if (nextRunInstant.equals(previous)) {
                // sleep just a half second so we can respond to stop requests
                SleepHalfSecond();
                continue;
            }

            // otherwise schedule the run task, and sleep until we're done
            logger.debug("scheduling next run on {}, current is {}", nextRunInstant, Instant.now());
            Date nextRun = Date.from(nextRunInstant);
            timer.schedule(new StartNewRoundTask(), nextRun);

            previous = nextRunInstant;
            SleepHalfSecond();
        }

        logger.debug("done with stop");
        timer.cancel();
        timer.purge();
    }

    protected void SleepHalfSecond() {
        long msToSleep = 500;
        try {
            Thread.sleep(msToSleep);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * timer task to start new round async.
     */
    protected class StartNewRoundTask extends TimerTask {
        @Override
        public void run() {
            logger.debug("running the start task");
            StartNewRound();
        }
    }
}
