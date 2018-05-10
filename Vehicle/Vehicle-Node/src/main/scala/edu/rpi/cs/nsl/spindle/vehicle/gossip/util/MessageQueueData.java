package edu.rpi.cs.nsl.spindle.vehicle.gossip.util;

/**
 * basically a typed pair for a message queue that is implemented as a list
 */
public class MessageQueueData {
    public String Sender;
    public Object Message;


    public MessageQueueData(String sender, Object message) {
        this.Sender = sender;
        this.Message = message;
    }
}
