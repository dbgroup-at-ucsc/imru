package edu.uci.ics.hyracks.imru.elastic;

import java.io.IOException;
import java.util.BitSet;
import java.util.Vector;

import edu.uci.ics.hyracks.imru.elastic.swap.LockRequest;
import edu.uci.ics.hyracks.imru.elastic.swap.ReleaseLock;
import edu.uci.ics.hyracks.imru.util.Rt;

public class AggrStates {
    public static boolean debug = false;
    public static int debugNetworkSpeed = 0;
    public static int debugNodeCount = 8;
    public static ImruSendOperator[] debugSendOperators = new ImruSendOperator[100];

    public static void printAggrTree() {
        if (!debug)
            return;
        StringBuilder sb = new StringBuilder();
        sb.append("Current aggregation tree\n");
        for (int i = 0; i < debugNodeCount; i++) {
            ImruSendOperator o = debugSendOperators[i];
            if (o == null)
                continue;
            if (o.aggr.targetPartition < -1) {
                sb.append(i + ":\t sent to " + o.aggr.sentPartition + "\t("
                        + o.aggr.log + ")\n");
                continue;
            }
            sb.append(i + ":\t(");
            for (int j : o.aggr.incomingPartitions) {
                if (o.aggr.receivingPartitions.get(j))
                    sb.append("*");
                sb.append(j + ",");
            }
            sb.append(") -> " + i + " -> " + o.aggr.targetPartition);
            sb.append(" " + o.runningMappers + " mappers");
            if (o.aggr.receivedMapResult)
                sb.append(" map");
            if (o.aggr.allChildrenFinished)
                sb.append(" ready");
            if (o.aggr.holding)
                sb.append(" hold");
            if (o.aggr.sending)
                sb.append(" sending");
            if (o.aggr.swappingTarget >= 0) {
                sb.append(" swapTo" + o.aggr.swappingTarget);
                if (o.aggr.isParentNodeOfSwapping)
                    sb.append("i");
            }
            sb.append("\t(" + o.aggr.log + ")\n");
        }
        synchronized (debugSendOperators) {
            Rt.np(sb);
        }
    }

    BitSet receivingPartitions = new BitSet();
    BitSet receivedPartitions = new BitSet();

    public boolean isPartitionFinished(int src) {
        return receivingPartitions.get(src) || receivedPartitions.get(src);
    }

    Object aggrSync = new Object();
    int targetPartition;
    StringBuilder log = new StringBuilder();
    int sentPartition;
    int[] incomingPartitions;
    //    String aggrResult;
    boolean holding = false;
    boolean sending = false;

    //for swapping
    boolean swapSucceed;
    boolean swapFailed;
    String failedReason;
    long failedReasonReceivedSize;
    BitSet swapFailedPartitions = new BitSet();
    int swappingTarget = -1;
    BitSet expectingReplies = new BitSet();
    int totalRepliesRemaining;
    boolean isParentNodeOfSwapping;
    BitSet holdSucceed = new BitSet();
    int[] successfullyHoldPartitions;
    int[] newChildren;

    //for debugging
    boolean receivedMapResult = false;
    boolean allChildrenFinished = false;

    // Keep records for debugging
    Vector<Integer> swaps = new Vector<Integer>();
    Vector<Long> swapsTime = new Vector<Long>();
    Vector<Integer> swapped = new Vector<Integer>();
    Vector<Long> swappedTime = new Vector<Long>();
    Vector<Integer> swapsFailed = new Vector<Integer>();
    Vector<String> swapFailedReason = new Vector<String>();
    Vector<Long> swapFailedTime = new Vector<Long>();

    Vector<Integer> lockingPartitions;
    ImruSendOperator so;

    public AggrStates(ImruSendOperator so) {
        this.so = so;
    }

    int lockIncomingPartitions(boolean isParentNode, int target)
            throws IOException {
        Vector<Integer> lockingPartitions = new Vector<Integer>();
        synchronized (aggrSync) {
            expectingReplies = new BitSet();
            for (int id : incomingPartitions) {
                if (!isPartitionFinished(id)) {
                    expectingReplies.set(id);
                    lockingPartitions.add(id);
                }
            }
            totalRepliesRemaining = lockingPartitions.size();
        }
        LockRequest r2 = new LockRequest();
        r2.isParentNode = isParentNode;
        r2.newTargetPartition = target;
        for (int id : lockingPartitions) {
            if (id < 0 || id >= so.nPartitions)
                throw new Error(id + " " + so.nPartitions);
            so.sendObj(id, r2);
        }
        this.lockingPartitions = lockingPartitions;
        return lockingPartitions.size();
    }

    void releaseIncomingPartitions() throws IOException {
        Vector<Integer> v;
        synchronized (aggrSync) {
            if (lockingPartitions == null)
                return;
            v = new Vector<Integer>(lockingPartitions);
            lockingPartitions = null;
        }
        ReleaseLock r2 = new ReleaseLock();
        for (int id : v)
            so.sendObj(id, r2);
    }

    String getIncomingPartitionsString() {
        StringBuilder sb = new StringBuilder();
        for (int i : incomingPartitions)
            sb.append(i + ",");
        return sb.toString();
    }

    void swapChildren(int[] remove, int[] add, int add2) {
        String old = getIncomingPartitionsString();
        BitSet r = new BitSet();
        BitSet a = new BitSet();
        if (remove != null) {
            for (int i : remove)
                r.set(i);
        }
        if (add != null) {
            for (int i : add)
                a.set(i);
        }
        Vector<Integer> v = new Vector<Integer>();
        for (int i : incomingPartitions) {
            if (a.get(i))
                throw new Error();
            if (!r.get(i))
                v.add(i);
        }
        if (add2 >= 0 && !a.get(add2))
            v.add(add2);
        if (add != null) {
            for (int i : add) {
                if (i != so.curPartition)
                    v.add(i);
            }
        }
        incomingPartitions = Rt.intArray(v);
        if (debug)
            Rt.p("*** SWAP " + so.curPartition + "'s children: " + old + " -> "
                    + getIncomingPartitionsString() + " target "
                    + targetPartition);
    }

    public void aggrStarted(int srcParition, int targetParition, int size,
            int total) {
        //                        Rt.p(targetParition + " recv " + sourceParition + " "
        //                                + size + "/" + total);
        receivingPartitions.set(srcParition);
        failedReasonReceivedSize = size;
    }

    public void completedAggr(int srcPartition, int thisPartition,
            int replyPartition) throws IOException {
        //        byte[] receivedResult = (byte[]) object;
        if (this.allChildrenFinished) {
            Rt.p("ERROR " + so.curPartition + " recv data from " + srcPartition
                    + " {"
                    //                    + MergedFrames.deserialize(receivedResult)
                    + "}");
        }
        synchronized (this.aggrSync) {
            //                String orgResult = aggrResult;
            //                addResult(receivedResult);
            //            so.io.add(receivedResult);
            if (this.receivedPartitions.get(srcPartition)) {
                new Error().printStackTrace();
            }
            this.receivedPartitions.set(srcPartition);
            //                if (debug)
            //                    Rt.p(context.getNodeId() + " received result from "
            //                            + object + ", " + orgResult + " + " + object
            //                            + " = " + aggrResult);
            this.aggrSync.notifyAll();
        }
        if (debug) {
            Rt.p(so.curPartition + " recv data from " + srcPartition + " {"
            //                    + MergedFrames.deserialize(receivedResult) 
                    + "}");
            printAggrTree();
        }
        if (this.totalRepliesRemaining > 0) {
            if (this.isParentNodeOfSwapping
                    && srcPartition == this.swappingTarget) {
                //The target partition sent everything
                this.swapFailed = true;
                this.failedReason = "complete";
            }
            so.incomingMessageProcessor.checkHoldingStatus();
            if (this.totalRepliesRemaining <= 0)
                so.incomingMessageProcessor.holdComplete();
        }
    }
}
