package edu.rpi.cs.nsl.spindle.vehicle.gossip;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.gossip.PushSum;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.network.ConnectionMap;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.network.NetworkLayer;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.protocol.GossipManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the gossip code...
 */
public class GossipRunner {

    public void Start(String ID) {
        Logger logger = LoggerFactory.getLogger(this.getClass());
        logger.debug("about to build connection map!");

        ConnectionMap connectionMap = new ConnectionMap();
        // star network hard coded
        String baseIP = "172.17.0.";
        int ipStart = 2;
        for(int i = 0; i < 4; ++i){
            String ipToAdd = baseIP + (ipStart+ i);
            logger.debug("trying to add node with string: {}", ipToAdd);
            connectionMap.AddNode("" + i, ipToAdd, 8085);
        }

        logger.debug("starting network layer!");
        NetworkLayer networkLayer = new NetworkLayer(ID, connectionMap.GetPortFromID(ID), connectionMap);
        networkLayer.start();

        logger.debug("going to build manager");
        GossipManager gossipManager = new GossipManager(ID);
        gossipManager.SetGossip(new PushSum());
        gossipManager.SetConnectionMap(connectionMap);
        gossipManager.SetNetworkLayer(networkLayer);
        gossipManager.SetGossipValue(Double.parseDouble(ID));

        gossipManager.setName("manager " + ID);

        try {
            logger.debug("about to start!");
            gossipManager.start();
            Thread.sleep(20000);
            logger.debug("value: " + gossipManager.getValue() + "\t weight: " + gossipManager.getWeight());

            gossipManager.finish();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
