package edu.rpi.cs.nsl.spindle.vehicle.gossip.util;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class ProtocolScheduler extends Thread {
    protected IGossipProtocol m_protocol;
    protected long windowSize;
    protected boolean running = false;
    Logger logger = LoggerFactory.getLogger(this.getClass());


    public ProtocolScheduler(IGossipProtocol protocol, long window) {
        this.m_protocol = protocol;
        this.windowSize = window;
    }

    @Override
    public void run() {
        running = true;
        while(running) {
            try {
                m_protocol.LeadGossip();
                long sleepTime = getPoisson(windowSize) * 20;
                logger.debug("sleeping {}", sleepTime);
                sleep(sleepTime);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void Finish() {
        running = false;
    }

    public int getPoisson(double lambda) {
        double L = Math.exp(-lambda);
        double p = 1.0;
        int k = 0;

        Random rng = new Random();
        do {
            k++;
            p *= rng.nextDouble();
        } while (p > L);

        return k - 1;
    }
}
