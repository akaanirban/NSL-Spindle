package edu.rpi.cs.nsl.spindle.vehicle.gossip.query;

import java.io.Serializable;

public class Query implements Comparable, Serializable {
    protected String m_operation;
    protected String m_item;

    public Query(String operation, String item) {
        this.m_operation = operation;
        this.m_item = item;
    }

    public static final Query BLANK_QUERY = new Query("blank", "blank");

    @Override
    public String toString() {
        return "[op=" + m_operation + ", item=" + m_item + "]";
    }

    @Override
    public int compareTo(Object o) {
        if(o != null) {
            return toString().compareTo(o.toString());
        }
        return 1;
    }
};

