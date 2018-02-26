package edu.rpi.cs.nsl.spindle.vehicle.gossip.network;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.*;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkObserver;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class NetworkLayer extends Thread implements INetworkSender, INetworkObserver {

    protected ConnectionMap connectionMap;

    protected ServerSocket serverSocket;
    protected boolean running;

    protected String myID;
    protected int myPort;

    protected ArrayList<INetworkObserver> observers;

    protected NetworkMessageBuffer buffer;

    // only build when there is an attempt to use
    protected ConcurrentHashMap<String, InSocketManager> inSocks;
    protected ConcurrentHashMap<String, OutSocketManager> outSocks;

    public NetworkLayer(String myID, int myPort, ConnectionMap connectionMap) {
        this.connectionMap = connectionMap;

        this.myID = myID;
        this.myPort = myPort;
        this.running = false;

        this.observers = new ArrayList<>();
        this.inSocks = new ConcurrentHashMap<>();
        this.outSocks = new ConcurrentHashMap<>();

        // set up the buffer./r
        this.buffer = new NetworkMessageBuffer();
        this.observers.add(this.buffer);
    }

    public void AddObserver(INetworkObserver observer) {
        buffer.SetObserver(observer);
    }

    @Override
    public void Send(String target, IGossipMessageData message) {
        // try to open the socket
        if(!outSocks.containsKey(target)) {
            boolean good = TryOpenSocket(target);
            if(!good) {
                logger.debug("failed to find, trying to build");
                logger.error("failed to find, trying to build");
                return;
            }
        }

        // try to send on the socket
        OutSocketManager manager = outSocks.get(target);
        logger.debug("{} sending {} to {} over {}\n",
                myID, message, target, manager);
        manager.Send(target, message);
    }

    protected boolean TryOpenSocket(String target) {
        try {
            logger.debug("trying to open socket to {}", target);
            InetSocketAddress addr = connectionMap.GetAddr(target);
            Socket socket = new Socket();
            socket.connect(addr, 300);
            OutSocketManager outManager = new OutSocketManager(target, myID, socket);

            // try to add to map
            outManager.AddObserver(this);

            logger.debug("created socket to: {}", target);
            outSocks.put(target, outManager);

        } catch(Exception e) {
            e.printStackTrace();
            // TODO: be sure to remove bad socket and indicate bad status
            logger.debug("error opening socket to {}: {}", target, e.getMessage());
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
            logger.debug("going to close: " + myID);
            serverSocket.close();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void OnNetworkActivity(String sender, Object message) {
        logger.debug("message {} from: {} got {}\n", myID, sender, message.toString());
        if(message instanceof StartUpMessage) {
            StartUpMessage startUpMessage = (StartUpMessage) message;

            // change the socket locations
            InSocketManager manager = inSocks.get(sender);
            inSocks.remove(sender);
            manager.SetID(startUpMessage.sourceID);
            inSocks.put(startUpMessage.sourceID, manager);

            logger.debug("{} fixed socket for {}\n", myID, startUpMessage.sourceID);


            // don't pass down
            return;
        }

        NotifyMessageObservers(sender, message);
    }

    @Override
    public void OnMessageStatus(UUID messageId, MessageStatus status) {
        // TODO Auto-generated method stub
        logger.debug("status {} from: {} got {}\n", myID, messageId, status);

        NotifyStatusObservers(messageId, status);
    }

    public void NotifyMessageObservers(String sender, Object message) {
        for(INetworkObserver observer : observers) {
            observer.OnNetworkActivity(sender, message);
        }
    }

    public void NotifyStatusObservers(UUID sender, MessageStatus status) {
        for(INetworkObserver observer : observers) {
            observer.OnMessageStatus(sender, status);
        }
    }
}
