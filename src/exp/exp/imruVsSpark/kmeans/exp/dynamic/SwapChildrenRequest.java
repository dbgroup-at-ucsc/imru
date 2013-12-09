package exp.imruVsSpark.kmeans.exp.dynamic;

import java.io.Serializable;

public class SwapChildrenRequest extends SwapCommand {
    int removePartition;
    int addPartition;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("remove=" + removePartition + ", add=" + addPartition);
        return "Swap " + sb.toString();
    }
}
