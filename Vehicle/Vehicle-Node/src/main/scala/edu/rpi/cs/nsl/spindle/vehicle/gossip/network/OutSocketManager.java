package edu.rpi.cs.nsl.spindle.vehicle.gossip.network;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkObserver;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkSender;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.util.MessageStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class OutSocketManager extends Thread implements INetworkSender {
    protected String myID;
    protected Socket socket;
    protected ArrayList<INetworkObserver> observers;
    protected ObjectOutputStream ostr;
    Logger logger = LoggerFactory.getLogger(this.getClass());
    protected Lock lock;


    protected boolean running;

    public OutSocketManager(String myID, String sourceID, Socket socket) {
        this.myID = myID;
        this.socket = socket;
        this.observers = new ArrayList<INetworkObserver>();
        this.lock = new ReentrantLock();

        // try to build the output stream
        try {
            ostr = new ObjectOutputStream(socket.getOutputStream());
            ostr.writeObject(new StartUpMessage(sourceID));
        } catch (IOException e) {
            logger.debug("io exception {} {}", e, e.getMessage());
            e.printStackTrace();
            try {
                socket.close();
            } catch (IOException e1) {
                logger.debug("io exception {} {}", e, e.getMessage());
                e1.printStackTrace();
            }
        }

        this.running = false;
    }

    public void AddObserver(INetworkObserver observer) {
        this.observers.add(observer);
    }

    public synchronized void NotifyStatusObservers(UUID messageId, MessageStatus status) {
        for (INetworkObserver observer : observers) {
            observer.OnMessageStatus(messageId, status);
        }
    }

    public void Close() {
        try {
            ostr.close();
            socket.close();
        } catch (IOException e) {
            logger.debug("io exception {} {}", e, e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public synchronized void Send(String target, IGossipMessageData message) {
        // try to send the message, do it async so we don't block
        new Thread(new Runnable() {

            @Override
            public void run() {
                DoSend(target, message);
            }

        }).start();
    }

    protected synchronized void DoSend(String target, IGossipMessageData message) {
        // try to send the message
        logger.debug("trying to send: " + message + " to: " + target);
        lock.lock();
        try {

            ostr.writeObject(message);

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("bad send of {} to {}, got exception {} with message {}", message, target, e, e.toString());

            lock.unlock();
            NotifyStatusObservers(message.GetUUID(), MessageStatus.BAD);
            return;
        }
        lock.unlock();
        NotifyStatusObservers(message.GetUUID(), MessageStatus.GOOD);
        logger.debug("good send of {} to {}", message, target);
    }
}
