package edu.uci.ics.hyracks.imru.elastic.map;

import edu.uci.ics.hyracks.imru.elastic.swap.DynamicCommand;

public class RequestSplit extends DynamicCommand {
    public int requestedBy;
    public int splitLocation;
    public int splitUUID;

    public RequestSplit(int requestedBy, int splitLocation, int splitUUID) {
        this.requestedBy = requestedBy;
        this.splitLocation = splitLocation;
        this.splitUUID = splitUUID;
    }

    @Override
    public String toString() {
        return "RequestSplit " + requestedBy + " " + splitLocation + " "
                + splitUUID;
    }
}
