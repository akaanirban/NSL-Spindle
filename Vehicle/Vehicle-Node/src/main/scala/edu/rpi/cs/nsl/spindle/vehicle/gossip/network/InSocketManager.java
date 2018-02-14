package edu.rpi.cs.nsl.spindle.vehicle.gossip.network;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.MessageStatus;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.ArrayList;

public class InSocketManager extends Thread {
	Logger logger = LoggerFactory.getLogger(this.getClass());

	protected String myID;
	protected Socket socket;
	protected ArrayList<INetworkObserver> observers;

	protected boolean running;
	
	public InSocketManager(String myID, Socket socket) {
		this.myID = myID;
		this.socket = socket;
		this.observers = new ArrayList<INetworkObserver>();
		
		this.running = false;
	}
	
	public void SetID(String ID) {
		this.myID = ID;
	}
	
	public void AddObserver(INetworkObserver observer) {
		this.observers.add(observer);
	}
	
	public void NotifyMessageObservers(Object message) {
		for(INetworkObserver observer : observers) {
			observer.OnNetworkActivity(myID, message);
		}
	}
	
	public void NotifyStatusObservers(MessageStatus status) {
		for(INetworkObserver observer : observers) {
			observer.OnMessageStatus(myID, status);
		}
	}
	
	public void Close() {
		logger.debug("trying to close");
		this.running = false;
	}
	
	public void run() {
		try {
			ObjectInputStream istr = new ObjectInputStream(socket.getInputStream());
			running = true;
			while(running) {
				//System.out.println("trying to read message");
				Object obj = istr.readObject();
				//System.out.println("got message: " + obj.toString());
				
				NotifyMessageObservers(obj);				
			}
			logger.debug("done running, trying to close");
			
			istr.close();
			socket.close();
			
		} catch(IOException e) {
			logger.debug("exception {}",e.getMessage());
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			logger.debug("exception {}", e.getMessage());
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
		    // be doubly sure we closed the socket
			socket.close();
		} catch (IOException e) {
		    logger.debug("error closing the socket");
			e.printStackTrace();
		}
		System.out.println("finished running isock: " + myID);
	}
}
