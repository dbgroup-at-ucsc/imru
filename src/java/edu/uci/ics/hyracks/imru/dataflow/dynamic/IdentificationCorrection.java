package edu.uci.ics.hyracks.imru.dataflow.dynamic;

public class IdentificationCorrection extends SwapCommand {
    int partition;
    int writer;

    public IdentificationCorrection(int partition, int writer) {
        this.partition = partition;
        this.writer = writer;
    }

    @Override
    public String toString() {
        return "ID " + partition + "->" + writer;
    }
}
