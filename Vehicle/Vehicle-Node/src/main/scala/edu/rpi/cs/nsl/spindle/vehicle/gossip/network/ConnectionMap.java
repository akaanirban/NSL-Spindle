package edu.rpi.cs.nsl.spindle.vehicle.gossip.network;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Random;
import java.util.Map.Entry;
import java.util.Set;

public class ConnectionMap {
	protected HashMap<String, Integer> nodes;
	
	public ConnectionMap() {
		nodes = new HashMap<String, Integer>();
	}
	
	public void AddNode(String node, int port) {
		nodes.put(node, port);
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
		
		System.out.println("couldn't find id for port: " + port);
		return "";
	}
	
	public InetSocketAddress GetAddr(String target) {
		int port = nodes.get(target);
		return new InetSocketAddress("127.0.0.1", port);
	}
	
	public String ChooseRandomTarget() {
		Set<String> keys = nodes.keySet();
		Random rng = new Random();
		return (String) keys.toArray()[rng.nextInt(keys.size())];
	}
}
