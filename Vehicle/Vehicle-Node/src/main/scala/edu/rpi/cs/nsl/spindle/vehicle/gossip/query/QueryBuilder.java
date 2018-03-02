package edu.rpi.cs.nsl.spindle.vehicle.gossip.query;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.gossip.Consensus;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipProtocol;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.protocol.ConsensusProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Use this class to configure how protocols and their respective gossip gets built
 */
public class QueryBuilder {
    Logger logger = LoggerFactory.getLogger(this.getClass());

    protected String m_id;

    public QueryBuilder(String id) {
        this.m_id = id;
    }

    /**
     * builds the protocol and gossip for this
     * does not set the network connection or wire up the networking
     *
     * @param query the query to build
     */
    public IGossipProtocol BuildGossipProtocolFor(Query query) {
        ConsensusProtocol protocol = new ConsensusProtocol(m_id);
        double value = Double.parseDouble(m_id);

        double weight = 1.0;
        if (query.m_operation.equalsIgnoreCase("sum")) {
            if (value == 0.0) {
                weight = 1.0;
            }
            else {
                weight = 0.0;
            }
        }

        logger.debug("{} building query {} with weight {}", m_id, query, weight);

        Consensus gossip = new Consensus(value, weight);
        protocol.SetGossip(gossip);

        return protocol;
    }
}
