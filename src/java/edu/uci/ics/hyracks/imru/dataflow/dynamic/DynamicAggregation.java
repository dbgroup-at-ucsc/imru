package edu.uci.ics.hyracks.imru.dataflow.dynamic;

import java.io.IOException;
import java.io.Serializable;
import java.util.BitSet;
import java.util.Vector;

import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.imru.api.ImruIterInfo;
import edu.uci.ics.hyracks.imru.data.MergedFrames;
import edu.uci.ics.hyracks.imru.data.SerializedFrames;
import edu.uci.ics.hyracks.imru.util.Rt;

public class DynamicAggregation<Model extends Serializable, Data extends Serializable> {
    ImruSendOperator<Model, Data> so;

    public DynamicAggregation(ImruSendOperator<Model, Data> sendOperator) {
        this.so = sendOperator;
    }

    boolean waitForAggregation() throws Exception {
        Thread.currentThread().setName("ImruSendOperator " + so.curPartition);
        if (so.debug)
            Rt.p("*** Partion " + so.curPartition
                    + " completing mapping (incoming="
                    + so.getIncomingPartitionsString() + " target="
                    + so.targetPartition + ")");
        while (true) {
            synchronized (so.aggrSync) {
                if (so.holding)
                    so.aggrSync.wait();
                else {
                    so.receivedMapResult = true;;
                    break;
                }
            }
        }
        so.swappingTarget = -1;
        long swapTime = System.currentTimeMillis() + so.maxWaitTimeBeforeSwap;
        while (true) {
            int unfinishedPartition = -1;
            synchronized (so.aggrSync) {
                boolean hasMissingPartitions = false;
                StringBuilder missing = new StringBuilder();
                for (int id : so.incomingPartitions) {
                    if (unfinishedPartition < 0
                            && !so.receivingPartitions.get(id)
                            && !so.swapFailedPartitions.get(id))
                        unfinishedPartition = id;
                    if (!so.receivedPartitions.get(id)) {
                        missing.append(id + ",");
                        hasMissingPartitions = true;
                    }
                }
                if (!so.diableSwapping && unfinishedPartition >= 0) {
                    if (System.currentTimeMillis() < swapTime) {
                        so.aggrSync.wait();
                        continue;
                    } else {
                        so.swappingTarget = unfinishedPartition;
                        so.swapSucceed = false;
                        so.swapFailed = false;
                    }
                } else {
                    if (hasMissingPartitions) {
                        if (so.debug)
                            Rt.p(so.curPartition + " still need " + missing);
                        so.aggrSync.wait();
                        if (so.debug)
                            Rt.p(so.curPartition + " wake up");
                    } else {
                        break;
                    }
                }
            }
            if (so.debug)
                Rt.p(so.curPartition + " " + so.swappingTarget);
            if (so.swappingTarget >= 0)
                performSwap();
        }
        so.imruSpec.reduceDbgInfoClose(so.dbgInfoRecvQueue);
        ImruIterInfo info = so.imruSpec.reduceClose(so.recvQueue);
        //        so.io.close();

        //TODO: check the following code, no need to wait?
        so.allChildrenFinished = true;
        if (so.debug)
            Rt.p(so.curPartition + " all children finished");
        //completed aggregation
        while (true) {
            synchronized (so.aggrSync) {
                if (so.holding)
                    so.aggrSync.wait();
                else {
                    so.sending = true;
                    break;
                }
            }
        }
        if (so.debug)
            Rt.p(so.curPartition + " wait for aggregated result");
        while (so.aggregatedResult == null) {
            synchronized (so.aggrSync) {
                if (so.aggregatedResult == null)
                    so.aggrSync.wait();
            }
        }
        //        so.future.get();
        if (so.debug)
            Rt.p(so.curPartition + " start sending");
        boolean isRoot = so.targetPartition < 0;
        if (isRoot) {
            ImruIterInfo imruRuntimeInformation = new ImruIterInfo(
                    so.imruContext);
            Model model = (Model) so.imruContext.getModel();
            if (model == null) {
                //                if (so.debug) {
                String s = MergedFrames.deserialize(so.aggregatedResult)
                        .toString();
                String[] ss = s.split(",+");
                BitSet bs = new BitSet();
                StringBuilder sb = new StringBuilder();
                for (String s2 : ss)
                    bs.set(Integer.parseInt(s2));
                for (int i = 0; i < so.debugNodeCount; i++)
                    if (!bs.get(i))
                        sb.append("missing " + i + ",");
                Rt.p("Partition " + so.curPartition
                        + " uploading final result "
                        //                    + aggregatedResult.length);
                        + ss.length + " " + sb);
                Rt.p("Model == null " + so.imruContext.getNodeId());
                so.imruConnection.uploadModel(so.modelName, ss.length + " "
                        + sb);
                //                }
            } else {
                Object recvQueue = so.imruSpec
                        .updateInit(so.imruContext, model);
                Object dbgInfoQueue = so.imruSpec.updateDbgInfoInit(
                        so.imruContext, recvQueue);
                so.imruSpec.updateReceive(so.curPartition, 0,
                        so.aggregatedResult.length, so.aggregatedResult,
                        recvQueue);
                byte[] infoData = JavaSerializationUtils.serialize(info);
                so.imruSpec.updateDbgInfoReceive(so.curPartition, 0,
                        infoData.length, infoData, dbgInfoQueue);

                so.imruSpec.updateDbgInfoClose(dbgInfoQueue);
                info = so.imruSpec.updateClose(recvQueue);
                Model updatedModel = so.imruSpec.getUpdatedModel();
                //                Vector<byte[]> v = new Vector<byte[]>();
                //                v.add(so.aggregatedResult);
                //                Model updatedModel = so.imruSpec.updateFrames(so.imruContext, v
                //                        .iterator(), model);
                //                long start = System.currentTimeMillis();
                info.currentIteration = so.imruContext.getIterationNumber();
                so.imruConnection.uploadModel(so.modelName, updatedModel);
                so.imruConnection.uploadDbgInfo(so.modelName, info);
                //                long end = System.currentTimeMillis();
            }
            //                Rt.p(model);
            //            LOG.info("uploaded model to CC " + (end - start) + " milliseconds");
        } else {
            if (so.debug) {
                so.printAggrTree();
                Rt.p("UPLOAD " + so.curPartition + " -> " + so.targetPartition
                        + " "
                        //                        + aggregatedResult.length);
                        + MergedFrames.deserialize(so.aggregatedResult));
            }
            SerializedFrames.serializeToFrames(so.imruContext, so.getWriter(),
                    so.aggregatedResult, so.curPartition, so.targetPartition,
                    so.imruContext.getNodeId() + " reduce " + so.curPartition
                            + " " + so.imruContext.getOperatorName());
            //                        so.sendData(so.targetPartition, so.aggregatedResult);
            SerializedFrames.serializeDbgInfo(so.imruContext, so.getWriter(),
                    info, so.curPartition, so.targetPartition);
        }
        so.sentPartition = so.targetPartition;
        so.targetPartition = -2;
        return isRoot;
    }

    void performSwap() throws Exception {
        if (so.debug) {
            so.printAggrTree();
            Rt.p("*** " + so.curPartition + " attempts to swap with "
                    + so.swappingTarget + " "
                    + so.getIncomingPartitionsString() + "->"
                    + so.targetPartition);
        }
        so.isParentNodeOfSwapping = true;
        so.newChildren = null;
        so.lockIncomingPartitions(true, so.swappingTarget);
        while (true) {
            if (so.debug)
                Rt.p(so.curPartition + " swapping " + so.swappingTarget + " "
                        + so.swapSucceed + " " + so.swapFailed);
            if (so.swappingTarget >= 0
                    && (so.receivingPartitions.get(so.swappingTarget) || so.receivedPartitions
                            .get(so.swappingTarget))) {
                if (so.debug)
                    Rt.p(so.curPartition + " swapping target finished");
                so.swapFailed = true;
            } else {
                synchronized (so.aggrSync) {
                    if (!so.swapSucceed && !so.swapFailed)
                        so.aggrSync.wait();
                }
            }

            if (so.debug)
                Rt.p(so.curPartition + " wake up");
            if (so.swapFailed) {
                synchronized (so.aggrSync) {
                    so.expectingReplies.clear();
                    so.totalRepliesRemaining = 0;
                }
                so.releaseIncomingPartitions();
                if (so.debug)
                    Rt.p("*** " + so.curPartition + " attempts to swap with "
                            + so.swappingTarget + " failed");
                so.swapFailedPartitions.set(so.swappingTarget);
                so.swappingTarget = -1;
                break;
            } else if (so.swapSucceed) {
                int target = so.targetPartition;
                SwapTargetRequest swap = new SwapTargetRequest();
                swap.outgoingPartitionOfSender = so.targetPartition;
                swap.newTargetPartition = so.swappingTarget;
                for (int id : so.successfullyHoldPartitions) {
                    if (id != so.swappingTarget) {
                        if (so.debug)
                            Rt.p(so.curPartition + "->" + so.targetPartition
                                    + " send swap to " + id + " " + swap);
                        so.sendObj(id, swap);
                    }
                }
                swap.newTargetPartition = so.curPartition;
                if (so.newChildren != null) {
                    for (int id : so.newChildren) {
                        if (so.debug)
                            Rt.p(so.curPartition + "->" + so.targetPartition
                                    + " send swap to " + id + " " + swap);
                        so.sendObj(id, swap);
                    }
                }
                if (so.debug)
                    Rt
                            .p(so.curPartition + "->" + so.targetPartition
                                    + " send swap to " + so.swappingTarget
                                    + " " + swap);
                swap.newTargetPartition = so.swappingTarget;
                swap.incompeleteIncomingPartitions = so.successfullyHoldPartitions;
                so.sendObj(so.swappingTarget, swap);
                so.targetPartition = so.swappingTarget;
                so.log.append("i" + so.targetPartition + ",");

                so.swapChildren(so.successfullyHoldPartitions, so.newChildren,
                        -1);
                if (target >= 0)
                    so.sendObj(target, new SwapChildrenRequest(so.curPartition,
                            so.swappingTarget));
//                Rt.p("swap "+ so.curPartition+" with "+ so.swappingTarget);
                so.swappingTarget = -1;
                break;
            }
        }
    }
}
