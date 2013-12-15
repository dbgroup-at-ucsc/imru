package edu.uci.ics.hyracks.imru.dataflow.dynamic;

import java.io.Serializable;

public class SwapChildrenRequest extends SwapCommand {
    int removePartition;
    int addPartition;

    public SwapChildrenRequest(int remove, int add) {
        this.removePartition = remove;
        this.addPartition = add;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("remove=" + removePartition + ", add=" + addPartition);
        return "Swap " + sb.toString();
    }
}
