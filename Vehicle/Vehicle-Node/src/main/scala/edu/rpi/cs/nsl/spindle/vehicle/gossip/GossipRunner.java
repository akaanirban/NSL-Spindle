package edu.rpi.cs.nsl.spindle.vehicle.gossip;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.gossip.Consensus;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.gossip.PushSum;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.network.ConnectionMap;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.network.NetworkLayer;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.protocol.ConsensusProtocol;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.protocol.PushSumProtocol;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.query.Query;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.query.QueryBuilder;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.util.ProtocolScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Runs the gossip code...
 */
public class GossipRunner {

    public void StartManagedGossipTwoQueriesWithEpoch(String ID, String countStr) {
        Logger logger = LoggerFactory.getLogger(this.getClass());
        logger.debug("about to build connection map!");

        ConnectionMap connectionMap = new ConnectionMap();
        // star network hard coded
        String baseIP = "172.17.0.";
        int numNodes = Integer.parseInt(countStr) + 1;
        int ipStart = 2;
        for (int i = 0; i < numNodes; ++i) {
            String ipToAdd = baseIP + (ipStart + i);
            logger.debug("trying to add node with string: {}", ipToAdd);
            connectionMap.AddNode("" + i, ipToAdd, 8085);
        }

        logger.debug("starting network layer!");
        NetworkLayer networkLayer = new NetworkLayer(ID, connectionMap.GetPortFromID(ID), connectionMap);
        networkLayer.start();
        try {
            // sleep to be sure everyone has started
            Thread.sleep(2000);

            QueryBuilder builder = new QueryBuilder(ID);
            Manager manager = new Manager(builder, connectionMap, networkLayer);

            Thread managerThread = new Thread(manager);

            manager.AddQuery(new Query("sum", "ids"));
            manager.AddQuery(new Query("avg", "ids"));
            logger.debug("build and started manager!");

            managerThread.start();

            Thread.sleep(30000);

            // now try to shut it down
            logger.debug("going to stop whole thing!");
            manager.Stop();

            // sleep to let anything finish
            manager.StopSchedulers();
            Thread.sleep(4000);

            logger.debug("going to get result");
            // now try to get the result, then shut down
            Map<Query, Object> results = manager.GetResults();
            logger.debug("result is {}", results);
            for (Map.Entry<Query, Object> result : results.entrySet()) {
                logger.debug("{} got result {}", result.getKey(), result.getValue());
            }

            managerThread.join();

            logger.debug("joined manager, going to stop protocols");
            manager.StopProtocols();

            logger.debug("trying to close server");
            networkLayer.closeServer();
            networkLayer.join();
            logger.debug("closed server");

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        logger.debug("all done, yay!");
    }

    public void StartManagedGossipTwoQueries(String ID, String countStr) {
        Logger logger = LoggerFactory.getLogger(this.getClass());
        logger.debug("about to build connection map!");

        ConnectionMap connectionMap = new ConnectionMap();
        // star network hard coded
        String baseIP = "172.17.0.";
        int numNodes = Integer.parseInt(countStr) + 1;
        int ipStart = 2;
        for (int i = 0; i < numNodes; ++i) {
            String ipToAdd = baseIP + (ipStart + i);
            logger.debug("trying to add node with string: {}", ipToAdd);
            connectionMap.AddNode("" + i, ipToAdd, 8085);
        }

        logger.debug("starting network layer!");
        NetworkLayer networkLayer = new NetworkLayer(ID, connectionMap.GetPortFromID(ID), connectionMap);
        networkLayer.start();
        try {
            // sleep to be sure everyone has started
            Thread.sleep(2000);

            QueryBuilder builder = new QueryBuilder(ID);
            Manager manager = new Manager(builder, connectionMap, networkLayer);

            //Thread managerThread = new Thread(manager);

            manager.AddQuery(new Query("sum", "ids"));
            manager.AddQuery(new Query("avg", "ids"));
            logger.debug("build and started manager!");

            //managerThread.start();
            manager.StartNewRound();

            Thread.sleep(30000);

            // now try to shut it down
            logger.debug("going to stop whole thing!");
            manager.Stop();

            // sleep to let anything finish
            manager.StopSchedulers();
            Thread.sleep(4000);

            logger.debug("going to get result");
            // now try to get the result, then shut down
            Map<Query, Object> results = manager.GetResults();
            logger.debug("result is {}", results);
            for (Map.Entry<Query, Object> result : results.entrySet()) {
                logger.debug("{} got result {}", result.getKey(), result.getValue());
            }

            //managerThread.join();

            logger.debug("joined manager, going to stop protocols");
            manager.StopProtocols();

            logger.debug("trying to close server");
            networkLayer.closeServer();
            networkLayer.join();
            logger.debug("closed server");

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        logger.debug("all done, yay!");
    }

    public void StartManagedGossip(String ID, String countStr) {
        Logger logger = LoggerFactory.getLogger(this.getClass());
        logger.debug("about to build connection map!");

        ConnectionMap connectionMap = new ConnectionMap();
        // star network hard coded
        String baseIP = "172.17.0.";
        int numNodes = Integer.parseInt(countStr) + 1;
        int ipStart = 2;
        for (int i = 0; i < numNodes; ++i) {
            String ipToAdd = baseIP + (ipStart + i);
            logger.debug("trying to add node with string: {}", ipToAdd);
            connectionMap.AddNode("" + i, ipToAdd, 8085);
        }

        logger.debug("starting network layer!");
        NetworkLayer networkLayer = new NetworkLayer(ID, connectionMap.GetPortFromID(ID), connectionMap);
        networkLayer.start();
        try {
            // sleep to be sure everyone has started
            Thread.sleep(2000);

            QueryBuilder builder = new QueryBuilder(ID);
            Manager manager = new Manager(builder, connectionMap, networkLayer);

            manager.AddQuery(Query.BLANK_QUERY);
            logger.debug("build and started manager!");
            Thread.sleep(15000);

            // now shut down the shedulers
            logger.debug("going to stop scheduler");
            manager.StopSchedulers();
            // sleep to let anything finish

            Thread.sleep(4000);

            logger.debug("going to get result");
            // now try to get the result, then shut down
            Map<Query, Object> results = manager.GetResults();
            logger.debug("result is {}", results);
            for (Map.Entry<Query, Object> result : results.entrySet()) {
                logger.debug("{} got result {}", result.getKey(), result.getValue());
            }

            manager.Stop();

            logger.debug("trying to close server");
            networkLayer.closeServer();
            networkLayer.join();
            logger.debug("closed server");

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        logger.debug("all done, yay!");
    }

    public void StartConsensusGossip(String ID, String countStr) {
        Logger logger = LoggerFactory.getLogger(this.getClass());
        logger.debug("about to build connection map!");

        ConnectionMap connectionMap = new ConnectionMap();
        // star network hard coded
        String baseIP = "172.17.0.";
        int numNodes = Integer.parseInt(countStr) + 1;
        int ipStart = 2;
        for (int i = 0; i < numNodes; ++i) {
            String ipToAdd = baseIP + (ipStart + i);
            logger.debug("trying to add node with string: {}", ipToAdd);
            connectionMap.AddNode("" + i, ipToAdd, 8085);
        }

        logger.debug("starting network layer!");
        NetworkLayer networkLayer = new NetworkLayer(ID, connectionMap.GetPortFromID(ID), connectionMap);
        networkLayer.start();
        try {
            // sleep to be sure everyone has started
            Thread.sleep(1000);

            logger.debug("going to build protocol");
            ConsensusProtocol protocol = new ConsensusProtocol(ID);
            logger.debug("going to build gossip with value: {}", ID);
            protocol.SetGossip(new Consensus(Double.parseDouble(ID), 1.0));
            protocol.SetNetwork(networkLayer);
            protocol.SetConnectionMap(connectionMap);

            logger.debug("setting protocol as network observer");
            networkLayer.AddObserver(protocol);

            ProtocolScheduler scheduler = new ProtocolScheduler(protocol, 20);
            logger.debug("built the scheduler");

            logger.debug("going to run the protocol");
            Thread.sleep(1000);
            Thread protocolThread = new Thread(protocol);
            protocolThread.start();

            logger.debug("started protocol");
            scheduler.start();
            Thread.sleep(20000);
            logger.debug("stopping lead to let in flight messages arrive");
            scheduler.Finish();
            scheduler.join();
            Thread.sleep(2000);
            logger.debug("value is: {}", protocol.GetGossip().GetValue());
            protocol.Stop();

            logger.debug("called stop, waiting to join");

            protocolThread.join();

            logger.debug("trying to close server");
            networkLayer.closeServer();
            networkLayer.join();
            logger.debug("closed server");

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        logger.debug("all done, yay!");
    }

    public void StartPushSumGossip(String ID, String countStr) {
        Logger logger = LoggerFactory.getLogger(this.getClass());
        logger.debug("about to build connection map!");

        ConnectionMap connectionMap = new ConnectionMap();
        // star network hard coded
        String baseIP = "172.17.0.";
        int numNodes = Integer.parseInt(countStr) + 1;
        int ipStart = 2;
        for (int i = 0; i < numNodes; ++i) {
            String ipToAdd = baseIP + (ipStart + i);
            logger.debug("trying to add node with string: {}", ipToAdd);
            connectionMap.AddNode("" + i, ipToAdd, 8085);
        }

        logger.debug("starting network layer!");
        NetworkLayer networkLayer = new NetworkLayer(ID, connectionMap.GetPortFromID(ID), connectionMap);
        networkLayer.start();

        logger.debug("going to build protocol");
        PushSumProtocol protocol = new PushSumProtocol(ID);
        logger.debug("going to build gossip with value: {}", ID);
        protocol.SetGossip(new PushSum(Double.parseDouble(ID), 1.0));
        protocol.SetNetwork(networkLayer);
        protocol.SetConnectionMap(connectionMap);

        logger.debug("setting protocol as network observer");
        networkLayer.AddObserver(protocol);


        ProtocolScheduler scheduler = new ProtocolScheduler(protocol, 20);
        logger.debug("built the scheduler");
        try {

            logger.debug("going to run the protocol");
            Thread.sleep(1000);
            Thread protocolThread = new Thread(protocol);
            protocolThread.start();

            logger.debug("started protocol");
            scheduler.start();
            Thread.sleep(20000);
            logger.debug("stopping lead to let in flight messages arrive");
            scheduler.Finish();
            scheduler.join();
            Thread.sleep(2000);
            logger.debug("value is: {}", protocol.GetGossip().GetValue());
            protocol.Stop();

            logger.debug("called stop, waiting to join");

            protocolThread.join();

            logger.debug("trying to close server");
            networkLayer.closeServer();
            networkLayer.join();
            logger.debug("closed server");

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        logger.debug("all done, yay!");
    }
}
