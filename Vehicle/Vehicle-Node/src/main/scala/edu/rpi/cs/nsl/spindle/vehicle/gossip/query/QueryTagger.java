package edu.rpi.cs.nsl.spindle.vehicle.gossip.query;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkSender;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.messages.QueryTaggedMessage;

/**
 * QueryTagger tags any messages it receives with its query then passes them on.
 */
public class QueryTagger implements INetworkSender {

    protected Query m_query;
    protected INetworkSender m_sender;

    public QueryTagger(Query query, INetworkSender sender) {
        this.m_query = query;
        this.m_sender = sender;
    }

    @Override
    public void Send(String target, IGossipMessageData message) {
        QueryTaggedMessage outputMessage = new QueryTaggedMessage(message, m_query);
        m_sender.Send(target, outputMessage);
    }
}
