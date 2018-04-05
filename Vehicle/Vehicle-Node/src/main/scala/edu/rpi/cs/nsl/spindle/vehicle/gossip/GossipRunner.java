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
        boolean useFlat = m_conf.getBoolean("spindle.vehicle.gossip.use-flat");
        boolean useNeighbors = m_conf.getBoolean("spindle.vehicle.gossip.use-neighbors");
        logger.debug("using flat: {}", useFlat);

        m_connectionMap = new ConnectionMap();
        String baseIP = "172.17.0.";
        int ipStart = 2;

        if (useNeighbors) {
            String path = "spindle.vehicle.neighbors.".concat(m_ID);
            logger.debug("going to get the path:", path);
            String neighbors = m_conf.getString(path);
            logger.debug("got neighbors: {}", neighbors);

            String[] neighborNodes = neighbors.trim().split("\\s+");

            for (String neighbor : neighborNodes) {
                logger.debug("going to parse: {}", neighbor);

                int idVal = Integer.parseInt(neighbor);
                String ipToAdd = baseIP + (ipStart + idVal);

                logger.debug("going to add {}", idVal);
                m_connectionMap.AddNode(neighbor, ipToAdd, 8085);
                logger.debug("done adding {} {}", neighbor, ipToAdd);
            }

            int idVal = Integer.parseInt(m_ID);
            String ipToAdd = baseIP + (ipStart + idVal);

            logger.debug("going to add {}", idVal);
            m_connectionMap.AddNode(m_ID, ipToAdd, 8085);
            logger.debug("done adding {} {}", m_ID, ipToAdd);
        }
        else if (useFlat) {
            int idVal = Integer.parseInt(m_ID);
            logger.debug("id valu is: {}", idVal);

            for (int i = 0; i < m_numberOfNodes; ++i) {
                String ipToAdd = baseIP + (ipStart + i);
                if (i == idVal - 1 || i == idVal || i == idVal + 1) {
                    logger.debug("flat graph adding node with string {} val {}", ipToAdd, i);
                    m_connectionMap.AddNode("" + i, ipToAdd, 8085);
                    logger.debug("going to next one");
                }
            }
        }
        else {
            // star network hard coded
            for (int i = 0; i < m_numberOfNodes; ++i) {
                String ipToAdd = baseIP + (ipStart + i);
                logger.debug("trying to add node with string: {}", ipToAdd);
                m_connectionMap.AddNode("" + i, ipToAdd, 8085);
            }
        }
    }

    public GossipResult GetResult() {
        return m_gossipResult;
    }

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
