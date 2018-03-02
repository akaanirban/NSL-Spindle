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
        for (INetworkObserver observer : observers) {
            logger.debug("{} notifying observer about: {}", myID, message);
            observer.OnNetworkActivity(myID, message);
            logger.debug("{} done notifying observer about {}", myID, message);
        }
    }

    public void NotifyStatusObservers(MessageStatus status) {
        logger.debug("error: shouldn't notify status observers from insocket!");
        assert (false);
    }

    public void Close() {
        logger.debug("trying to close");
        this.running = false;
    }

    public void run() {
        logger.debug("starting to run inmanager for: {}", myID);
        try {
            ObjectInputStream istr = new ObjectInputStream(socket.getInputStream());
            running = true;
            while (running) {
                logger.debug("trying to read obj for {}", myID);
                //System.out.println("trying to read message");
                Object obj = istr.readObject();
                //System.out.println("got message: " + obj.toString());
                logger.debug("{} got message {}", myID, obj);
                NotifyMessageObservers(obj);
            }
            logger.debug("done running, trying to close");

            istr.close();
            socket.close();

        } catch (IOException e) {
            logger.debug("io exception {} {}", e, e.getMessage());
            logger.error("io exception {}", e.getMessage(), e);

            e.printStackTrace();
        } catch (Exception e) {
            logger.debug("unknown exception {} {}", e, e.getMessage());
            logger.error("unknown exception {}", e.getMessage(), e);

            e.printStackTrace();
        }

        try {
            // be doubly sure we closed the socket
            socket.close();
        } catch (IOException e) {
            logger.debug("error closing socket for {}: {} {}", myID, e, e.getMessage());
            e.printStackTrace();
        }

        logger.debug("finished running isock: {}", myID);
    }
}
