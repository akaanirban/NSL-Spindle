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

        result.clear();
        logger.debug("built {} for query {}", result, queryID);

        return result;
    }

    public Map<String, Double> GetResultWithDefault() {
        Map<Query, Object> rawMap = m_gossipResult.GetResult();
        Map<String, Double> result = new TreeMap<>();

        // check that there is a map
        if (rawMap == null) {
            logger.debug("uh oh, raw map is null!");
            return result;
        }

        Query speedQuery = new Query("avg", "ids");
        Query countQuery = new Query("sum", "ids");

        Object spd = rawMap.getOrDefault(speedQuery, 0.0);
        Object cnt = rawMap.getOrDefault(countQuery, 0.0);

        try {
            Double speedD = (Double) spd;
            Double countD = (Double) cnt;
            result.put("speed", speedD);
            result.put("count", countD);

        } catch (Exception e) {
            logger.error("failed to cast", e);
        }

        // check that our queries are in the map
        if (!(rawMap.containsKey(speedQuery) && rawMap.containsKey(countQuery))) {
            logger.debug("rawmap {} doesn't contain one query, returning empty", rawMap);
            return result;
        }

        logger.debug("built {} for query {}", result, "spd cnt");

        return result;
    }
}
