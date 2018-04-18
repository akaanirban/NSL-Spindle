package edu.rpi.cs.nsl.spindle.vehicle.gossip.results;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class GossipResult {
    Logger logger = LoggerFactory.getLogger(this.getClass());

    protected Lock m_lock;
    protected Map<Query, Object> m_result;

    public GossipResult() {
        m_lock = new ReentrantLock();
    }

    public void SetResult(Map<Query, Object> result) {
        logger.debug("updating the results!");
        m_lock.lock();
        logger.debug("got the lock!");
        m_result = result;
        m_lock.unlock();
        logger.debug("done updating the results!");
    }

    public Map<Query, Object> GetResult() {
        // leaking reference... happens unless we deep copy the map or take the k-v as an input
        // TODO: fix this
        return m_result;
    }
}
