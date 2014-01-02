package edu.uci.ics.hyracks.imru.dataflow.dynamic;

public class IdentifyRequest extends SwapCommand {
    int src;
    int dest;

    public IdentifyRequest(int src, int dest) {
        this.src = src;
        this.dest = dest;
    }

    @Override
    public String toString() {
        return "ID " + src + "->" + dest;
    }
}
