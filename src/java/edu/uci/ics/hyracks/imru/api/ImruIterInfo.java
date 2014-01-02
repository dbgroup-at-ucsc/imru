package edu.uci.ics.hyracks.imru.api;

import java.io.Serializable;
import java.util.BitSet;
import java.util.Date;
import java.util.HashSet;
import java.util.Vector;

import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * IMRU iteration debugging information
 * 
 * @author Rui Wang
 */
public class ImruIterInfo implements Serializable {
    public static final long serialVersionUID = 1;

    public static class OperatorInfo implements Serializable {
        public static final long serialVersionUID = 1;
        public String nodeId;
        public String operator;
        public int partition;
        public int totalPartitions;
        public Vector<OperatorInfo> childrens = new Vector<OperatorInfo>();
        public String completedPath;
        public long mappedDataSize;
        public int mappedRecords;
        public long totalMappedDataSize;
        public int totalMappedRecords;

        public long operatorStartTime;
        public long operatorTotalTime;

        //for reducer and updater
        public long totalRecvData;
        public long totalRecvTime;
        public long totalProcessed;
        public long totalProcessTime;

        //for dynamic aggregation
        public int[] swapsWithPartitions; // initiate and complete swapping with partitions
        public long[] swapsTime;
        public int[] swappedWithPartitions; // being swapped with partitions
        public long[] swappedTime;
        public String[] swapsFailed; // initiate but failed
        public long[] swapsFailedTime;

        public OperatorInfo(String nodeId, String operator, int partition,
                int totalPartitions) {
            this.nodeId = nodeId;
            this.operator = operator;
            this.partition = partition;
            this.totalPartitions = totalPartitions;
        }

        public void copyTo(OperatorInfo target) {
            target.completedPath = completedPath;
            target.mappedDataSize = mappedDataSize;
            target.mappedRecords = mappedRecords;
        }

        public long findMinTime() {
            long t = operatorStartTime;
            for (OperatorInfo info : childrens) {
                long t2 = info.findMinTime();
                if (t2 < t)
                    t = t2;
            }
            return t;
        }
    }

    public int currentIteration = -1;
    public int finishedRecoveryIteration = 0;
    public boolean dynamicAggregationEnabled = false;
    public boolean swappingDisabled = false;
    public long maxWaitTimeBeforeSwap = 0;

    // public T object;
    public Vector<String> allCompletedPaths = new Vector<String>();
    public OperatorInfo op;

    public ImruIterInfo(IMRUContext ctx) {
        this(ctx.getNodeId(), ctx.getOperatorName(), ctx.getPartition(), ctx
                .getPartitions());
    }

    public ImruIterInfo(String nodeId, String operator, int partition,
            int totalPartitions) {
        op = new OperatorInfo(nodeId, operator, partition, totalPartitions);
    }

    public void add(ImruIterInfo r2) {
        if (this.currentIteration < 0)
            this.currentIteration = r2.currentIteration;
        this.op.totalMappedDataSize += r2.op.totalMappedDataSize;
        this.op.totalMappedRecords += r2.op.totalMappedRecords;
        HashSet<String> hash = new HashSet<String>();
        for (String s : allCompletedPaths)
            hash.add(s);
        for (Object object : r2.allCompletedPaths) {
            String s = (String) object;
            if (!hash.contains(s)) {
                hash.add(s);
                this.allCompletedPaths.add(s);
            }
        }
        // if (r2.aggrTree.nodeId.equals(this.aggrTree.nodeId)) {
        // same node
        // }
        //        Rt.p("add " + r2.aggrTree.operator + " to " + aggrTree.operator);
        op.childrens.add(r2.op);
    }

    private void printAggrTree(OperatorInfo info, StringBuilder sb, BitSet bs,
            int level, long minTime) {
        for (int i = 0; i < level; i++) {
            if (i < level - 1)
                sb.append(bs.get(i) ? "   " : "|  ");
            else
                sb.append(bs.get(i) ? "+--" : "+--");
        }
        sb.append(info.operator + " (" + info.partition + "/"
                + info.totalPartitions + ") " + info.nodeId);
        boolean mapper = info.operator.contains("map")
                || info.childrens.size() == 0;
        if (mapper) {
            sb.append(String.format(" mapped=%,d (%,d)", info.mappedRecords,
                    info.mappedDataSize));
        } else {
            sb.append(String.format(" count=%,d size=%.1fMB",
                    info.totalMappedRecords,
                    info.totalMappedDataSize / 1024.0 / 1024.0));
        }
        //start time duration
        sb.append(String.format(" st=%.3fs du=%.3fs",
                (info.operatorStartTime - minTime) / 1000.0,
                (info.operatorTotalTime) / 1000.0));
        if (mapper) {
            sb.append(String.format(" %.1fR/s %.1fMB/s", info.mappedRecords
                    * 1000.0 / info.operatorTotalTime, info.mappedDataSize
                    / 1024.0 / 1024.0 * 1000.0 / info.operatorTotalTime));
        } else {
            sb.append(String.format(" recv=%.2fMB(%.1fMB/s)",
                    info.totalRecvData / 1024.0 / 1024.0, info.totalRecvData
                            * 1000.0 / 1024.0 / 1024.0 / info.totalRecvTime));
            sb.append(String.format(" aggr=%d(%.1fcnt/s)", info.totalProcessed,
                    info.totalProcessed * 1000.0 / info.totalProcessTime));
        }
        if (info.swapsWithPartitions != null
                && info.swapsWithPartitions.length > 0) {
            sb.append(" swaps=(");
            for (int i = 0; i < info.swapsWithPartitions.length; i++)
                sb.append(info.swapsWithPartitions[i]
                        + String.format("[%.3fs]",
                                (info.swapsTime[i] - minTime) / 1000.0) + ",");
            sb.append(")");
        }
        if (info.swappedWithPartitions != null
                && info.swappedWithPartitions.length > 0) {
            sb.append(" swapped=(");
            for (int i = 0; i < info.swappedWithPartitions.length; i++)
                sb
                        .append(info.swappedWithPartitions[i]
                                + String
                                        .format(
                                                "[%.3fs]",
                                                (info.swappedTime[i] - minTime) / 1000.0)
                                + ",");
            sb.append(")");
        }
        if (info.swapsFailed != null) {
            sb.append(" swapsFail=(");
            for (int i = 0; i < info.swapsFailed.length; i++)
                sb.append(info.swapsFailed[i]
                        + String.format("[%.3fs]",
                                (info.swapsFailedTime[i] - minTime) / 1000.0)
                        + ",");
            sb.append(")");
        }
        if (info.completedPath != null) {
            sb.append("\n");
            for (int i = 0; i < level; i++) {
                if (i < level - 1)
                    sb.append(bs.get(i) ? "   " : "|  ");
                else
                    sb.append(bs.get(i) ? "   " : "   ");
            }
            sb.append(" ").append(info.completedPath);
        }
        sb.append("\n");
        bs.set(level, info.childrens.size() == 0);
        for (OperatorInfo n : info.childrens) {
            bs.set(level, n == info.childrens.lastElement());
            printAggrTree(n, sb, bs, level + 1, minTime);
        }
    }

    public String getAggrTree() {
        BitSet bs = new BitSet();
        StringBuilder sb = new StringBuilder();
        long minTime = op.findMinTime();
        sb.append("Start time: " + new Date(minTime) + "\n");
        printAggrTree(op, sb, bs, 0, minTime);
        return sb.toString();
    }

    public String getReport() {
        StringBuilder sb = new StringBuilder();
        sb.append("Processed paths:\n");
        for (String s : allCompletedPaths)
            sb.append(" " + s + "\n");

        sb.append("\n");
        sb.append("current iteration: " + currentIteration + "\n");
        sb.append("current recovery iteration: " + finishedRecoveryIteration
                + "\n");
        sb.append("map data size: " + op.totalMappedDataSize + "\n");
        sb.append("map records: " + op.totalMappedRecords + "\n");
        sb.append("dynamic aggregation: " + this.dynamicAggregationEnabled
                + "\n");
        sb.append("disable swapping: " + this.swappingDisabled + "\n");
        sb.append("wait before swapping: " + this.maxWaitTimeBeforeSwap + "\n");

        sb.append("\n");
        sb.append("Data flow graph:\n");
        sb.append(getAggrTree() + "\n");
        return sb.toString();
    }

    public void printReport() {
        Rt.np(getReport());
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
