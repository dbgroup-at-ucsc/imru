package exp.imruVsSpark.kmeans.exp.dynamic;

import java.io.Serializable;

public class SwapTargetRequest extends SwapCommand {
    int outgoingPartitionOfSender;
    int newTargetPartition;
    int[] incompeleteIncomingPartitions; // of this partition

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
