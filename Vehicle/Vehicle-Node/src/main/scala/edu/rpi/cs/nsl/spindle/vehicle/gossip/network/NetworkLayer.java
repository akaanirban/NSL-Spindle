package edu.rpi.cs.nsl.spindle.vehicle.gossip.network;

import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.IGossipMessageData;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkObserver;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.interfaces.INetworkSender;
import edu.rpi.cs.nsl.spindle.vehicle.gossip.util.MessageStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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

    protected Lock sendLock;
    protected Lock recvLock;

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

        this.sendLock = new ReentrantLock();
        this.recvLock = new ReentrantLock();

    }

    public void AddObserver(INetworkObserver observer) {
        buffer.SetObserver(observer);
    }

    @Override
    public void Send(String target, IGossipMessageData message) {
        // try to open the socket
        sendLock.lock();
        if (!outSocks.containsKey(target)) {
            boolean good = TryOpenSocket(target);
            if (!good) {
                logger.debug("failed to open socket to {} for message {}", target, message);
                sendLock.unlock();

                NotifyStatusObservers(message.GetUUID(), MessageStatus.BAD);
                // send message back up
                return;
            }
        }


        // try to send on the socket
        OutSocketManager manager = outSocks.get(target);
        logger.debug("{} sending {} to {} over {}", myID, message, target, manager);
        sendLock.unlock();
        manager.Send(target, message);
    }

    /**
     * trys to open socket to target, note that it doesn't report status back up
     *
     * @param target
     * @return
     */
    protected synchronized boolean TryOpenSocket(String target) {
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

        } catch (Exception e) {
            e.printStackTrace();
            logger.debug("error opening socket to {}: {}", target, e.getMessage());
            return false;
        }

        return true;
    }

    public void closeServer() {
        running = false;
        try {
            serverSocket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void run() {
        try {
            running = true;
            this.serverSocket = new ServerSocket(myPort);

            while (running) {
                Socket socket = serverSocket.accept();
                // just to the port for temp
                int port = socket.getPort();

                String tempID = "" + port;
                logger.debug("adding insocket with temp id {}", tempID);
                // add to map
                recvLock.lock();
                InSocketManager manager = new InSocketManager(tempID, socket);

                inSocks.put(tempID, manager);
                recvLock.unlock();

                manager.AddObserver(this);
                manager.start();

                logger.debug("done adding insocket with temp id {}", tempID);
            }

            logger.debug("closing socket server {}" + myID);
            serverSocket.close();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("got exception, possibly failed to close server", e);
        }
    }

    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public synchronized void OnNetworkActivity(String sender, Object message) {
        logger.debug("message {} from: {} got {}\n", myID, sender, message.toString());
        if (message instanceof StartUpMessage) {
            StartUpMessage startUpMessage = (StartUpMessage) message;

            // change the socket locations
            logger.debug("insocks: {}", inSocks);
            recvLock.lock();
            if (!inSocks.containsKey(sender)) {
                logger.error("ERROR: could not find sender {} in insocks! Message: {} ", sender, message);
            }

            InSocketManager manager = inSocks.get(sender);
            inSocks.remove(sender);
            manager.SetID(startUpMessage.sourceID);
            inSocks.put(startUpMessage.sourceID, manager);

            recvLock.unlock();

            logger.debug("{} fixed socket for {}\n", myID, startUpMessage.sourceID);

            // don't pass up
            return;
        }

        NotifyMessageObservers(sender, message);
    }

    @Override
    public synchronized void OnMessageStatus(UUID messageId, MessageStatus status) {
        logger.debug("status {} from {} got {}\n", myID, messageId, status);

        NotifyStatusObservers(messageId, status);
    }

    public synchronized void NotifyMessageObservers(String sender, Object message) {
        for (INetworkObserver observer : observers) {
            observer.OnNetworkActivity(sender, message);
        }
    }

    public synchronized void NotifyStatusObservers(UUID sender, MessageStatus status) {
        for (INetworkObserver observer : observers) {
            observer.OnMessageStatus(sender, status);
        }
    }
}
