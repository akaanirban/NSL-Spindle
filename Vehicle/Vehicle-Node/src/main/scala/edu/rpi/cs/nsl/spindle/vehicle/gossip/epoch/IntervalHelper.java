package edu.rpi.cs.nsl.spindle.vehicle.gossip.epoch;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * get next instant that occurs at specific period (in seconds)
 */
public class IntervalHelper {
    protected long m_period;

    public IntervalHelper(int period) {
        this.m_period = period;
    }

    public Instant GetCurrentInterval() {
        Instant nextInstant = GetNext();
        Instant currentInstant = nextInstant.minusSeconds(m_period);

        return currentInstant;
    }

    public Instant GetNext() {
        Instant now = Instant.now();
        long sinceEpoch = now.getEpochSecond();
        long secondInterval = sinceEpoch % 60;

        // now figure out # seconds to next
        long remainingSeconds = m_period - (secondInterval % m_period);

        // chop seconds off the start, add this
        Instant next = now.plusSeconds(remainingSeconds);

        return next.truncatedTo(ChronoUnit.SECONDS);
    }
}
