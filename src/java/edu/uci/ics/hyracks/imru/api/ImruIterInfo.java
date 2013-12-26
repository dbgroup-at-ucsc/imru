package edu.uci.ics.hyracks.imru.api;

import java.io.Serializable;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Vector;

import edu.uci.ics.hyracks.ec2.Rt;

/**
 * IMRU iteration debugging information
 * 
 * @author Rui Wang
 */
public class ImruIterInfo implements Serializable {
    public static class OperatorInfo implements Serializable {
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
    }

    public int currentIteration = -1;
    public int finishedRecoveryIteration = 0;

    // public T object;
    public Vector<String> allCompletedPaths = new Vector<String>();
    public OperatorInfo aggrTree;

    public ImruIterInfo(IMRUContext ctx) {
        this(ctx.getNodeId(), ctx.getOperatorName(), ctx.getPartition(), ctx
                .getPartitions());
    }

    public ImruIterInfo(String nodeId, String operator, int partition,
            int totalPartitions) {
        aggrTree = new OperatorInfo(nodeId, operator, partition,
                totalPartitions);
    }

    public void add(ImruIterInfo r2) {
        if (this.currentIteration < 0)
            this.currentIteration = r2.currentIteration;
        this.aggrTree.totalMappedDataSize += r2.aggrTree.totalMappedDataSize;
        this.aggrTree.totalMappedRecords += r2.aggrTree.totalMappedRecords;
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
        aggrTree.childrens.add(r2.aggrTree);
    }

    private void printAggrTree(OperatorInfo info, StringBuilder sb, BitSet bs,
            int level) {
        for (int i = 0; i < level; i++) {
            if (i < level - 1)
                sb.append(bs.get(i) ? "   " : "|  ");
            else
                sb.append(bs.get(i) ? "+--" : "+--");
        }
        sb.append(info.operator + " (" + info.partition + "/"
                + info.totalPartitions + ") " + info.nodeId);
        if (info.operator.contains("map") || info.childrens.size() == 0) {
            sb.append(String.format(" mapped=%,d (%,d)", info.mappedRecords,
                    info.mappedDataSize));
        } else {
            sb.append(String.format(" total=%,d (%,d)",
                    info.totalMappedRecords, info.totalMappedDataSize));
        }
        if (info.completedPath != null)
            sb.append(" ").append(info.completedPath);
        sb.append("\n");
        bs.set(level, info.childrens.size() == 0);
        for (OperatorInfo n : info.childrens) {
            bs.set(level, n == info.childrens.lastElement());
            printAggrTree(n, sb, bs, level + 1);
        }
    }

    public String getAggrTree() {
        BitSet bs = new BitSet();
        StringBuilder sb = new StringBuilder();
        printAggrTree(aggrTree, sb, bs, 0);
        return sb.toString();
    }

    public void printReport() {
        Rt.p("Completed paths:");
        for (String s : allCompletedPaths)
            Rt.np(" "+s);
        Rt.np("current iteration: " + currentIteration);
        Rt.np("current recovery iteration: " + finishedRecoveryIteration);
        Rt.np("map data size: " + aggrTree.totalMappedDataSize);
        Rt.np("map records: " + aggrTree.totalMappedRecords);
        Rt.np(getAggrTree());
    }

    @Override
    public String toString() {
        // if (object != null)
        // return object.toString();
        return super.toString();
    }
}
