package edu.uci.ics.hyracks.imru.dataflow.dynamic.swap;

import java.io.Serializable;

public class LockRequest extends DynamicCommand {
    public boolean isParentNode;
    public int newTargetPartition;

    @Override
    public String toString() {
        return (isParentNode ? "" : "*") + "Lock swapTo=" + newTargetPartition;
    }
}