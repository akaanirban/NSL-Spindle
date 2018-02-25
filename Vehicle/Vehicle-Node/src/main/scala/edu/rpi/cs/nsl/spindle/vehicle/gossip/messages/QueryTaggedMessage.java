package edu.rpi.cs.nsl.spindle.vehicle.gossip.messages;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.query.Query;

public class QueryTaggedMessage extends NestedMessage {

    protected Query m_query;

    public QueryTaggedMessage(IGossipMessageData message, Query query) {
        super(message);
        this.m_query = query;
    }

    public Query GetQuery() {
        return m_query;
    }

    @Override
    public String toString() {
        return "[type=qtm,query=" + m_query.toString() + super.toString() + "]";
    }
}
