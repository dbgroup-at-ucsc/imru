package edu.uci.ics.hyracks.imru.dataflow.dynamic.swap;


public class IdentificationCorrection extends DynamicCommand {
    public int partition;
    public int writer;

    public IdentificationCorrection(int partition, int writer) {
        this.partition = partition;
        this.writer = writer;
    }

    @Override
    public String toString() {
        return "ID " + partition + "->" + writer;
    }
}
