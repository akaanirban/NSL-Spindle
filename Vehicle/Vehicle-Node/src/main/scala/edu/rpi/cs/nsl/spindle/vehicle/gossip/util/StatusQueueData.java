package edu.rpi.cs.nsl.spindle.vehicle.gossip.util;

import java.util.UUID;

public class StatusQueueData {
    protected UUID MessageId;
    protected Object Message;

    public StatusQueueData(UUID messageId, Object message) {
        this.MessageId = messageId;
        this.Message = message;
    }

    public UUID GetMessageId() {
        return MessageId;
    }

    public Object GetMessage() {
        return Message;
    }

}
