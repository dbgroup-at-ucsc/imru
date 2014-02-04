package edu.uci.ics.hyracks.imru.dataflow.dynamic.map;

import java.util.Vector;

import edu.uci.ics.hyracks.imru.dataflow.dynamic.swap.DynamicCommand;

public class ReplyAvailableSplit extends DynamicCommand {
    public int requestedBy;
    public long uuid;
    public int splitLocation;
    public int splitUUID;
    public Vector<Integer> forwardedPartitions = new Vector<Integer>();

    public ReplyAvailableSplit(GetAvailableSplits get, int curPartition,
            int splitUUID) {
        this.requestedBy = get.requestedBy;
        this.uuid = get.uuid;
        this.splitLocation = curPartition;
        this.splitUUID = splitUUID;
        forwardedPartitions.addAll(get.forwardedPartitions);
    }

    @Override
    public String toString() {
        return "ReplyAvailableSplit request=" + requestedBy + " location="
                + splitLocation + " split=" + splitUUID;
    }
}
