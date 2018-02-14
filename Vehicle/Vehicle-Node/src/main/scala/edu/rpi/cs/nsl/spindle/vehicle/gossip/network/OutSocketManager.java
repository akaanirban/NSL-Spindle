package edu.rpi.cs.nsl.spindle.vehicle.gossip.network;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.MessageStatus;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.StartUpMessage;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkObserver;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;

public class OutSocketManager extends Thread implements INetworkSender {
	protected String myID;
	protected Socket socket;
	protected ArrayList<INetworkObserver> observers;
	protected ObjectOutputStream ostr;
	Logger logger = LoggerFactory.getLogger(this.getClass());


	protected boolean running;
	
	public OutSocketManager(String myID, String sourceID, Socket socket) {
		this.myID = myID;
		this.socket = socket;
		this.observers = new ArrayList<INetworkObserver>();
		
		// try to build the output stream
		try {
			ostr = new ObjectOutputStream(socket.getOutputStream());
			ostr.writeObject(new StartUpMessage(sourceID));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			try {
				socket.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		
		this.running = false;
	}
	
	public void AddObserver(INetworkObserver observer) {
		this.observers.add(observer);
	}
	
	public void NotifyStatusObservers(MessageStatus status) {
		for(INetworkObserver observer : observers) {
			observer.OnMessageStatus(myID, status);
		}
	}
	
	public void Close() {
		try {
			ostr.close();
			socket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void Send(String target, Object message) {
		// try to send the message, need to do it async
		new Thread(new Runnable() {

			@Override
			public void run() {
				// try to send the message
				logger.debug("trying to send: " + message + " to: " + target);
				try {
					//socket.setSoTimeout(300);
					//ObjectOutputStream ostr = new ObjectOutputStream(socket.getOutputStream());
					ostr.writeObject(message);
					//ostr.close();
				} catch(Exception e) {
					NotifyStatusObservers(MessageStatus.BAD);
					e.printStackTrace();
					return;
				}
				NotifyStatusObservers(MessageStatus.GOOD);
				logger.debug("good send of {} to {}", message, target);
			}
			
		}).start();
	}
	
	
	
}
