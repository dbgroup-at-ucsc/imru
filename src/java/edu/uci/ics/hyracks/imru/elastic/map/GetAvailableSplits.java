package edu.uci.ics.hyracks.imru.elastic.map;

import java.util.Random;
import java.util.Vector;

import edu.uci.ics.hyracks.imru.elastic.swap.DynamicCommand;

public class GetAvailableSplits extends DynamicCommand {
    public int requestedBy;
    public long uuid;
    public Vector<Integer> forwardedPartitions = new Vector<Integer>();

    public GetAvailableSplits(int requestedBy) {
        this.requestedBy = requestedBy;
        this.uuid = (new Random().nextLong() << 42)
                + ((long) requestedBy << 32)
                + (System.currentTimeMillis() & 0xFFFFFFFFL);
    }

    public GetAvailableSplits clone() {
        GetAvailableSplits s = new GetAvailableSplits(requestedBy);
        s.uuid = uuid;
        s.forwardedPartitions.addAll(this.forwardedPartitions);
        return s;
    }

    @Override
    public String toString() {
        return "GetAvailableSplits " + requestedBy;
    }
}
