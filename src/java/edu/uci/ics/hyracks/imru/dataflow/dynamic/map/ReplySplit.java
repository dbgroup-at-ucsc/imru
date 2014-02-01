package edu.uci.ics.hyracks.imru.dataflow.dynamic.map;

import edu.uci.ics.hyracks.imru.dataflow.dynamic.swap.DynamicCommand;

public class ReplySplit extends DynamicCommand {
    public int requestedBy;
    public int splitLocation;
    public int splitUUID;
    public boolean relocate;

    public ReplySplit(int requestedBy, int splitLocation, int splitUUID,
            boolean relocate) {
        this.requestedBy = requestedBy;
        this.splitLocation = splitLocation;
        this.splitUUID = splitUUID;
        this.relocate = relocate;
    }

    @Override
    public String toString() {
        return "ReplySplit " + requestedBy + " " + splitLocation + " "
                + splitUUID + " " + relocate;
    }
}
