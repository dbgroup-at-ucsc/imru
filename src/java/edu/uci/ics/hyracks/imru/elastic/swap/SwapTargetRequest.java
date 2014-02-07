package edu.uci.ics.hyracks.imru.elastic.swap;

import java.io.Serializable;

public class SwapTargetRequest extends DynamicCommand {
    public int outgoingPartitionOfSender;
    public int newTargetPartition;
    public  int[] incompeleteIncomingPartitions; // of this partition

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("up=" + outgoingPartitionOfSender + ", target="
                + newTargetPartition + ", children=[");
        if (incompeleteIncomingPartitions != null) {
            for (int i : incompeleteIncomingPartitions)
                sb.append(i + ",");
        }
        sb.append("]");
        return "Swap " + sb.toString();
    }
}
