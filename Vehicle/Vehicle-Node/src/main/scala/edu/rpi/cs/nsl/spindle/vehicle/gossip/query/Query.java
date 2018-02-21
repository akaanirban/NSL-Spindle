package edu.rpi.cs.nsl.spindle.vehicle.gossip.query;


public class Query {
    protected String m_operation;
    protected String m_item;

    public Query(String operation, String item) {
        this.m_operation = operation;
        this.m_item = item;
    }

    public static final Query BLANK_QUERY = new Query("blank", "blank");
};

