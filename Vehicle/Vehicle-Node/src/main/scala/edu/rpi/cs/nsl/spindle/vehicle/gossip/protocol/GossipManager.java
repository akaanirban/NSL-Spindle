package edu.rpi.cs.nsl.spindle.vehicle.gossip.protocol;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.gossip.NormalGossip;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.network.ConnectionMap;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.network.NetworkLayer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class GossipManager extends Thread {
	protected NormalGossip gossip;
	protected NetworkLayer layer;
	protected long windowSize;
	protected boolean running;
	protected ConnectionMap connectionMap;
	protected String myID;
	Logger logger = LoggerFactory.getLogger(this.getClass());


	public GossipManager(String myID) {
		this.running = false;
		this.myID = myID;
	}

	public void SetGossip(NormalGossip gossip) {
		this.gossip = gossip;
		this.gossip.setWindowSizeMS(0);
	}
	public void SetNetworkLayer(NetworkLayer layer) {
		gossip.SetNetworkSender(layer);
		layer.AddObserver(gossip);
		this.layer = layer;
	}

	public void SetGossipValue(Double value) {
		gossip.SetValue(value);
	}
	
	public void SetGossipWeight(double weight) {
		gossip.SetWeight(weight);
	}

	public void SetConnectionMap(ConnectionMap connectionMap) {
		this.connectionMap = connectionMap;
	}

	public double getValue() {
		return gossip.GetValue();
	}

	public double getWeight() {
		return gossip.GetWeigth();
	}
	
	public void finish() {
		running = false;
	}

	public void run() {
		running = true;

		// assume the network layer is already started

		while (running) {
			try {
				String target = myID;
				while (target.equals(myID)) {
					target = connectionMap.ChooseRandomTarget();
				}
				
				long sleepTime = getPoisson(10) * 20;
				sleep(sleepTime);


                logger.debug(myID + " going to gossip with " + target + " after: " + sleepTime);
				gossip.LeadGossip(target);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

        logger.debug("closing: " + this);
        logger.debug("layer: " + layer);
		layer.closeServer();
		try {
			layer.join(300);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        logger.debug("closed: " + this);
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
