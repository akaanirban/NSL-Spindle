package edu.rpi.cs.nsl.spindle.vehicle.gossip.util;

public class MessageQueueData {
    public String Sender;
    public Object Message;


    public MessageQueueData(String sender, Object message) {
        this.Sender = sender;
        this.Message = message;
    }
}
