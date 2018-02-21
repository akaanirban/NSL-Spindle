package edu.rpi.cs.nsl.spindle.vehicle.gossip;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.gossip.Consensus;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.gossip.PushSum;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.network.ConnectionMap;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.network.NetworkLayer;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.protocol.ConsensusProtocol;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.protocol.PushSumProtocol;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.util.ProtocolScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the gossip code...
 */
public class GossipRunner {


    public void StartConsensusGossip(String ID, String countStr) {
        Logger logger = LoggerFactory.getLogger(this.getClass());
        logger.debug("about to build connection map!");

        ConnectionMap connectionMap = new ConnectionMap();
        // star network hard coded
        String baseIP = "172.17.0.";
        int numNodes = Integer.parseInt(countStr) + 1;
        int ipStart = 2;
        for(int i = 0; i < numNodes; ++i){
            String ipToAdd = baseIP + (ipStart+ i);
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
        for(int i = 0; i < numNodes; ++i){
            String ipToAdd = baseIP + (ipStart+ i);
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
