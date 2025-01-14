package edu.rpi.cs.nsl.spindle.vehicle.gossip;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.network.ConnectionMap;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.network.NetworkLayer;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.query.Query;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.query.QueryBuilder;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.results.GossipResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton for setting up the gossip. This builds the queries and neighbors. Change this to change how gossip gets
 * set up.
 */
public class GossipRunner {
    Logger logger = LoggerFactory.getLogger(this.getClass());
    protected Config m_conf = ConfigFactory.load();

    private static volatile GossipRunner singletonInstance = null;

    protected String m_ID;
    protected double m_numberOfNodes;

    protected ConnectionMap m_connectionMap;
    protected NetworkLayer m_networkLayer;
    protected QueryBuilder m_queryBuilder;
    protected Manager m_manager;
    protected Thread m_managerThread;

    protected GossipResult m_gossipResult;

    public static GossipRunner GetInstance() {
        return singletonInstance;
    }

    public static void TryStart(String ID, String numberOfNodes) {
        if (singletonInstance == null) {
            singletonInstance = new GossipRunner(ID, numberOfNodes);
            singletonInstance.Start();
        }
    }

    public GossipRunner(String ID, String numberOfNodes) {
        m_ID = ID;
        m_numberOfNodes = Integer.parseInt(numberOfNodes) + 1;
        m_gossipResult = new GossipResult();
    }

    protected void BuildConnectionMap() {
        logger.debug("going to build connection map");
        m_connectionMap = new ConnectionMap();

        String clusterID = m_conf.getString("spindle.vehicle.cluster.which-cluster");
        Integer portNumber = m_conf.getInt("spindle.vehicle.gossip.port");
        logger.debug("the cluster string is: {}", clusterID);
        String baseName = "SPINDLE-CLUSTER" + clusterID + "-";

        // fully connected network hardcoded
        for (int i = 0; i < m_numberOfNodes; ++i) {
            String nameToAdd = "NODE" + i;
            if (i == 0) {
                nameToAdd = "CLUSTERHEAD";
            }
            String ipToAdd = baseName + nameToAdd;
            logger.debug("adding node with address: {}", ipToAdd);
            m_connectionMap.AddNode("" + i, ipToAdd, portNumber);
        }
        logger.debug("DONE building connection map!");
    }

    public GossipResult GetResult() {
        return m_gossipResult;
    }

    /**
     * builds the dependencies for gossip
     * NOTE: this is where the queries are built
     */
    protected void Start() {
        BuildConnectionMap();
        logger.debug("going to build the network layer");
        m_networkLayer = new NetworkLayer(m_ID, m_connectionMap.GetPortFromID(m_ID), m_connectionMap);
        logger.debug("done building going to start");
        m_networkLayer.start();
        logger.debug("starting network layer");

        // sleep after starting
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            logger.error("error sleeping", e);
        }

        m_queryBuilder = new QueryBuilder(m_ID);

        m_manager = new Manager(m_queryBuilder, m_connectionMap, m_networkLayer, m_gossipResult);
        m_manager.AddQuery(new Query("sum", "ids"));
        m_manager.AddQuery(new Query("avg", "ids"));

        m_managerThread = new Thread(m_manager);
        m_managerThread.start();
    }

    public void Stop() {
        logger.debug("going to stop");
        try {
            m_manager.Stop();
            m_managerThread.join();

            logger.debug("done closing manager");

            m_networkLayer.closeServer();
            m_networkLayer.join();

            logger.debug("done closing server");
        } catch (Exception e) {
            logger.error("error closing gossip: ", e);
        }

        logger.debug("all done closing gossip ");
    }
}
