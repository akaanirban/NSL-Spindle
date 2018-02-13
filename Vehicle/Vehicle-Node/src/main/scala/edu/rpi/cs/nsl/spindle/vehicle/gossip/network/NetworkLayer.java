package edu.rpi.cs.nsl.spindle.vehicle.gossip.network;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.*;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkObserver;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkSender;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class NetworkLayer extends Thread implements INetworkSender, INetworkObserver {

	protected ConnectionMap connectionMap;
	
	protected ServerSocket serverSocket;
	protected boolean running;
	
	protected String myID;
	protected int myPort;
	
	protected ArrayList<INetworkObserver> observers;
	
	// only build when there is an attempt to use
	protected ConcurrentHashMap<String, InSocketManager> inSocks;
	protected ConcurrentHashMap<String, OutSocketManager> outSocks;
	
	public NetworkLayer(String myID, int myPort, ConnectionMap connectionMap) {
		this.connectionMap = connectionMap;
		
		this.myID = myID;
		this.myPort = myPort;
		this.running = false;
		
		this.observers = new ArrayList<INetworkObserver>();
		this.inSocks = new ConcurrentHashMap<String, InSocketManager>();
		this.outSocks = new ConcurrentHashMap<String, OutSocketManager>();
	}
	
	public void AddObserver(INetworkObserver observer) {
		this.observers.add(observer);
	}
	
	@Override
	public void Send(String target, Object message) {
		// try to open the socket
		if(!outSocks.containsKey(target)) {
			boolean good = TryOpenSocket(target);
			if(!good) {
				System.out.println("failed to find, trying to build");
				return;
			}
		}
		
		// try to send on the socket
		OutSocketManager manager = outSocks.get(target);
		System.out.printf("%s sending %s to %s over %s\n", 
				myID, message, target, manager);
		manager.Send(target, message);
	}
	
	protected boolean TryOpenSocket(String target) {
		try {
			
			InetSocketAddress addr = connectionMap.GetAddr(target);
			Socket socket = new Socket();
			socket.connect(addr, 300);
			OutSocketManager outManager = new OutSocketManager(target, myID, socket);

			// try to add to map
			outManager.AddObserver(this);
			
			outSocks.put(target, outManager);
			
		} catch(Exception e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	public void closeServer() {
		running = false;
		try {
			serverSocket.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public void run() {
		try {
			running = true;
			this.serverSocket = new ServerSocket(myPort);
			
			while(running) {
				Socket socket = serverSocket.accept();
				// just to the port for temp
				int port = socket.getPort();

				String tempID = "" + port;
				// add to map
				InSocketManager manager = new InSocketManager(tempID, socket);
				manager.AddObserver(this);
				manager.start();
				
				// TODO: check if in map
				inSocks.put(tempID, manager);
			}
			System.out.println("going to close: " + myID);
			serverSocket.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void OnNetworkActivity(String sender, Object message) {
		// TODO Auto-generated method stub
		System.out.printf("message %s from: %s got %s\n", myID, sender, message.toString());
		if(message instanceof StartUpMessage) {
			StartUpMessage startUpMessage = (StartUpMessage) message;
			
			// change the socket locations
			InSocketManager manager = inSocks.get(sender);
			inSocks.remove(sender);
			manager.SetID(startUpMessage.sourceID);
			inSocks.put(startUpMessage.sourceID, manager);
			
			System.out.printf("%s fixed socket for %s\n", myID, startUpMessage.sourceID);
			
			
			// don't pass down
			return;
		}
		
		NotifyMessageObservers(sender, message);
	}
	
	@Override
	public void OnMessageStatus(String target, MessageStatus status) {
		// TODO Auto-generated method stub	
		System.out.printf("status %s from: %s got %s\n", myID, target, status);

		NotifyStatusObservers(target, status);
	}
	
	public void NotifyMessageObservers(String sender, Object message) {
		for(INetworkObserver observer : observers) {
			observer.OnNetworkActivity(sender, message);
		}
	}
	
	public void NotifyStatusObservers(String sender, MessageStatus status) {
		for(INetworkObserver observer : observers) {
			observer.OnMessageStatus(sender, status);
		}
	}
}
