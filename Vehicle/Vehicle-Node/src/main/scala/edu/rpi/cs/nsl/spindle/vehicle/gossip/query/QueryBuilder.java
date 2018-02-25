package edu.rpi.cs.nsl.spindle.vehicle.gossip.query;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.gossip.Consensus;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipProtocol;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.protocol.ConsensusProtocol;

/**
 * Knows how to build a gossip and protocol about a certain query
 */
public class QueryBuilder {

    protected String m_id;
    public QueryBuilder(String id) {
        this.m_id = id;
    }

    /**
     * builds the protocol and gossip for this
     * does not set the network connection or wire up the networking
     * @param query
     */
    public IGossipProtocol BuildGossipProtocolFor(Query query) {
        ConsensusProtocol protocol = new ConsensusProtocol(m_id);
        double value = Double.parseDouble(m_id);

        Consensus gossip = new Consensus(value, 1.0);
        protocol.SetGossip(gossip);

        return protocol;
    }
}
