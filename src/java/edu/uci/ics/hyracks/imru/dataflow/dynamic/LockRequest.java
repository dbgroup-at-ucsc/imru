package edu.uci.ics.hyracks.imru.dataflow.dynamic;

import java.io.Serializable;

public class LockRequest extends SwapCommand {
    boolean isParentNode;
    int newTargetPartition;

    @Override
    public String toString() {
        return (isParentNode ? "" : "*")+"Lock swapTo="+newTargetPartition;
    }
}