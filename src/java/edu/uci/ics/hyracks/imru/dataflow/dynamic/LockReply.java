package edu.uci.ics.hyracks.imru.dataflow.dynamic;

import java.io.Serializable;

public class LockReply extends SwapCommand {
    boolean forParentNode;
    boolean successful;
    String reason;
    int[] holdedIncomingPartitions; // of the swapped partition

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (!forParentNode)
            sb.append("*");
        sb.append(successful ? "y" : "n");
        sb.append(" locked=[");
        if (holdedIncomingPartitions != null) {
            for (int i : holdedIncomingPartitions)
                sb.append("," + i);
        }
        sb.append("]");
        return "Reply "+sb.toString();
    }
}
