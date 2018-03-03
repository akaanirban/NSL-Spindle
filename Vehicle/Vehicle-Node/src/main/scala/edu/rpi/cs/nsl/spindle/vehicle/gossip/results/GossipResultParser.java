package edu.rpi.cs.nsl.spindle.vehicle.gossip.results;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TreeMap;

public class GossipResultParser<K, V> {
    // actually does the conversion

    Logger logger = LoggerFactory.getLogger(this.getClass());
    protected GossipResult m_gossipResult;

    public GossipResultParser(GossipResult gossipResult) {
        m_gossipResult = gossipResult;
    }

    public Map<K, V> GetResult(String queryID) {
        Map<Query, Object> rawMap = m_gossipResult.GetResult();
        Map<K, V> result = new TreeMap<>();

        // check that there is a map
        if (rawMap == null) {
            logger.debug("uh oh, raw map is null!");
            return result;
        }

        Query speedQuery = new Query("avg", "ids");
        Query countQuery = new Query("sum", "ids");

        // check that our queries are in the map
        if (!(rawMap.containsKey(speedQuery) && rawMap.containsKey(countQuery))) {
            logger.debug("rawmap {} doesn't contain one query, returning empty", rawMap);
            return result;
        }

        Object kObj = rawMap.get(speedQuery);
        Object vObj = rawMap.get(countQuery);

        try {
            K kval = (K) kObj;
            V vval = (V) vObj;

            result.put(kval, vval);
        } catch (Exception e) {
            logger.error("failed to build kv pair for query {}:", queryID, e);
        }

        logger.debug("built {} for query {}", result, queryID);

        return result;
    }
}
