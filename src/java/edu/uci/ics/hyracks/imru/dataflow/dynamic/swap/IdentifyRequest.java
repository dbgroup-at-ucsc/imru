package edu.uci.ics.hyracks.imru.dataflow.dynamic.swap;


public class IdentifyRequest extends DynamicCommand {
    public int src;
    public int dest;

    public IdentifyRequest(int src, int dest) {
        this.src = src;
        this.dest = dest;
    }

    @Override
    public String toString() {
        return "ID " + src + "->" + dest;
    }
}
