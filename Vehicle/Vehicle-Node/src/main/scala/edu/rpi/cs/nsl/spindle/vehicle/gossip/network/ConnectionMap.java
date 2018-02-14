package edu.rpi.cs.nsl.spindle.vehicle.gossip.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Random;
import java.util.Map.Entry;
import java.util.Set;

public class ConnectionMap {
	protected HashMap<String, Integer> nodes;
	protected HashMap<String, String> ips;
	Logger logger = LoggerFactory.getLogger(this.getClass());


	public ConnectionMap() {
		nodes = new HashMap<String, Integer>();
		ips = new HashMap<>();
	}
	
	public void AddNode(String node, String ip, int port) {
		nodes.put(node, port);
		ips.put(node, ip);
	}
	
	public HashMap<String, Integer> GetNodes() {
		return this.nodes;
	}
	
	public int GetPortFromID(String ID) {
		return nodes.get(ID);
	}
	
	public String GetIDFromPort(int port) {
		for(Entry<String, Integer> pair : nodes.entrySet()) {
			if(pair.getValue() == port){
				return pair.getKey();
			}
		}
		
		logger.debug("couldn't find id for port: " + port);
		return "";
	}
	
	public InetSocketAddress GetAddr(String target) {
		int port = nodes.get(target);
		String ip = ips.get(target);
		logger.debug("ip for {} is {} on port {}", target, ip, port);
		return new InetSocketAddress(ip, port);
	}
	
	public String ChooseRandomTarget() {
		Set<String> keys = nodes.keySet();
		Random rng = new Random();
		return (String) keys.toArray()[rng.nextInt(keys.size())];
	}
}
