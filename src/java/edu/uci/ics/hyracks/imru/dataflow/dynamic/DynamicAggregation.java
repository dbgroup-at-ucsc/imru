package edu.uci.ics.hyracks.imru.dataflow.dynamic;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.BitSet;

import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.imru.api.ImruIterInfo;
import edu.uci.ics.hyracks.imru.data.SerializedFrames;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.swap.SwapChildrenRequest;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.swap.SwapTargetRequest;
import edu.uci.ics.hyracks.imru.util.Rt;

public class DynamicAggregation<Model extends Serializable, Data extends Serializable> {
    ImruSendOperator<Model, Data> so;

    public DynamicAggregation(ImruSendOperator<Model, Data> sendOperator) {
        this.so = sendOperator;
    }

    boolean waitForAggregation() throws Exception {
        Thread.currentThread().setName("ImruSendOperator " + so.curPartition);
        if (so.aggr.debug)
            Rt.p("*** Partion " + so.curPartition
                    + " completing mapping (incoming="
                    + so.aggr.getIncomingPartitionsString() + " target="
                    + so.aggr.targetPartition + ")");
        while (true) {
            synchronized (so.aggr.aggrSync) {
                if (so.aggr.holding)
                    so.aggr.aggrSync.wait();
                else {
                    so.aggr.receivedMapResult = true;;
                    break;
                }
            }
        }
        so.aggr.swappingTarget = -1;
        long swapTime = System.currentTimeMillis() + so.parameters.maxWaitTimeBeforeSwap;
        while (true) {
            int unfinishedPartition = -1;
            synchronized (so.aggr.aggrSync) {
                boolean hasMissingPartitions = false;
                StringBuilder missing = new StringBuilder();
                for (int id : so.aggr.incomingPartitions) {
                    if (unfinishedPartition < 0 && !so.aggr.isPartitionFinished(id)
                            && !so.aggr.swapFailedPartitions.get(id))
                        unfinishedPartition = id;
                    if (!so.aggr.receivedPartitions.get(id)) {
                        missing.append(id + ",");
                        hasMissingPartitions = true;
                    }
                }
                if (!so.parameters.disableSwapping && unfinishedPartition >= 0) {
                    if (System.currentTimeMillis() < swapTime) {
                        int waitTime = (int) (swapTime - System
                                .currentTimeMillis()) / 2;
                        if (waitTime < 1)
                            waitTime = 1;
                        so.aggr.aggrSync.wait(waitTime);
                        continue;
                    } else {
                        so.aggr.swappingTarget = unfinishedPartition;
                        so.aggr.swapSucceed = false;
                        so.aggr.swapFailed = false;
                        so.aggr.failedReason = null;
                    }
                } else {
                    if (hasMissingPartitions) {
                        if (so.aggr.debug)
                            Rt.p(so.curPartition + " still need " + missing);
                        so.aggr.aggrSync.wait();
                        if (so.aggr.debug)
                            Rt.p(so.curPartition + " wake up");
                    } else {
                        break;
                    }
                }
            }
            if (so.aggr.debug)
                Rt.p(so.curPartition + " " + so.aggr.swappingTarget);
            if (so.aggr.swappingTarget >= 0)
                performSwap();
        }
        so.imruSpec.reduceDbgInfoClose(so.dbgInfoRecvQueue);
        ImruIterInfo info = so.imruSpec.reduceClose(so.recvQueue);
        //        so.io.close();

        //TODO: check the following code, no need to wait?
        so.aggr.allChildrenFinished = true;
        if (so.aggr.debug)
            Rt.p(so.curPartition + " all children finished");
        //completed aggregation
        while (true) {
            synchronized (so.aggr.aggrSync) {
                if (so.aggr.holding)
                    so.aggr.aggrSync.wait();
                else {
                    so.aggr.sending = true;
                    break;
                }
            }
        }
        if (so.aggr.debug)
            Rt.p(so.curPartition + " wait for aggregated result");
        while (so.aggregatedResult == null) {
            synchronized (so.aggr.aggrSync) {
                if (so.aggregatedResult == null)
                    so.aggr.aggrSync.wait();
            }
        }
        //        so.future.get();
        if (so.aggr.debug)
            Rt.p(so.curPartition + " start sending");
        boolean isRoot = so.aggr.targetPartition < 0;
        info.op.swapsWithPartitions = Rt.intArray(so.aggr.swaps);
        info.op.swapsTime = Rt.longArray(so.aggr.swapsTime);
        info.op.swappedWithPartitions = Rt.intArray(so.aggr.swapped);
        info.op.swappedTime = Rt.longArray(so.aggr.swappedTime);
        if (so.aggr.swapsFailed.size() > 0) {
            info.op.swapsFailed = new String[so.aggr.swapsFailed.size()];
            info.op.swapsFailedTime = new long[so.aggr.swapsFailed.size()];
            for (int i = 0; i < so.aggr.swapsFailed.size(); i++) {
                info.op.swapsFailed[i] = so.aggr.swapsFailed.get(i) + ":"
                        + so.aggr.swapFailedReason.get(i);
                info.op.swapsFailedTime[i] = so.aggr.swapFailedTime.get(i);
            }
        }
        if (isRoot) {
            ImruIterInfo imruRuntimeInformation = new ImruIterInfo(
                    so.imruContext);
            Model model = (Model) so.imruContext.getModel();
            if (model == null) {
                //                if (so.aggr.debug) {
                String s = SerializedFrames.deserialize(so.aggregatedResult)
                        .toString();
                String[] ss = s.split(",+");
                BitSet bs = new BitSet();
                StringBuilder sb = new StringBuilder();
                for (String s2 : ss)
                    bs.set(Integer.parseInt(s2));
                for (int i = 0; i < so.aggr.debugNodeCount; i++)
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
                info.dynamicAggregationEnabled = true;
                info.swappingDisabled = so.parameters.disableSwapping;
                info.maxWaitTimeBeforeSwap = so.parameters.maxWaitTimeBeforeSwap;
                so.imruConnection.uploadModel(so.modelName, updatedModel);
                so.imruConnection.uploadDbgInfo(so.modelName, info);
                //                long end = System.currentTimeMillis();
            }
            //                Rt.p(model);
            //            LOG.info("uploaded model to CC " + (end - start) + " milliseconds");
        } else {
            if (so.aggr.debug) {
                so.aggr.printAggrTree();
                Rt.p("UPLOAD " + so.curPartition + " -> " + so.aggr.targetPartition
                        + " " + so.aggregatedResult.length
                //                        + MergedFrames.deserialize(so.aggregatedResult)
                        );
            }
            ByteBuffer frame = so.imruContext.allocateFrame();
            SerializedFrames.serializeToFrames(so.imruContext, frame,
                    so.imruContext.getFrameSize(), so.getWriter(),
                    so.aggregatedResult, so.curPartition, so.aggr.targetPartition,
                    so.curPartition, so.imruContext.getNodeId() + " reduce "
                            + so.curPartition + " "
                            + so.imruContext.getOperatorName(), so
                            .partitionToWriter(so.aggr.targetPartition));
            //                        so.sendData(so.targetPartition, so.aggregatedResult);
            SerializedFrames.serializeDbgInfo(so.imruContext, so.getWriter(),
                    info, so.curPartition, so.aggr.targetPartition, so
                            .partitionToWriter(so.aggr.targetPartition));
        }
        so.aggr.sentPartition = so.aggr.targetPartition;
        so.aggr.targetPartition = -2;
        return isRoot;
    }

    void performSwap() throws Exception {
        if (so.aggr.debug) {
            so.aggr.printAggrTree();
            Rt.p("*** " + so.curPartition + " attempts to swap with "
                    + so.aggr.swappingTarget + " "
                    + so.aggr.getIncomingPartitionsString() + "->"
                    + so.aggr.targetPartition);
        }
        so.aggr.isParentNodeOfSwapping = true;
        so.aggr.newChildren = null;
        so.aggr.lockIncomingPartitions(true, so.aggr.swappingTarget);
        while (true) {
            if (so.aggr.debug)
                Rt.p(so.curPartition + " swapping " + so.aggr.swappingTarget + " "
                        + so.aggr.swapSucceed + " " + so.aggr.swapFailed);
            if (so.aggr.swappingTarget >= 0
                    && (so.aggr.isPartitionFinished(so.aggr.swappingTarget))) {
                if (so.aggr.debug)
                    Rt.p(so.curPartition + " swapping target finished");
                //Already received from the target partition
                so.aggr.swapFailed = true;
                so.aggr.failedReason = "recv" + so.aggr.failedReasonReceivedSize;
            } else {
                synchronized (so.aggr.aggrSync) {
                    if (!so.aggr.swapSucceed && !so.aggr.swapFailed)
                        so.aggr.aggrSync.wait();
                }
            }

            if (so.aggr.debug)
                Rt.p(so.curPartition + " wake up");
            if (so.aggr.swapFailed) {
                synchronized (so.aggr.aggrSync) {
                    so.aggr.expectingReplies.clear();
                    so.aggr.totalRepliesRemaining = 0;
                }
                so.aggr.releaseIncomingPartitions();
                if (so.aggr.debug)
                    Rt.p("*** " + so.curPartition + " attempts to swap with "
                            + so.aggr.swappingTarget + " failed");
                so.aggr.swapFailedPartitions.set(so.aggr.swappingTarget);
                so.aggr.swapsFailed.add(so.aggr.swappingTarget);
                so.aggr.swapFailedReason.add(so.aggr.failedReason);
                so.aggr.swapFailedTime.add(System.currentTimeMillis());
                so.aggr.swappingTarget = -1;
                break;
            } else if (so.aggr.swapSucceed) {
                int target = so.aggr.targetPartition;
                SwapTargetRequest swap = new SwapTargetRequest();
                swap.outgoingPartitionOfSender = so.aggr.targetPartition;
                swap.newTargetPartition = so.aggr.swappingTarget;
                for (int id : so.aggr.successfullyHoldPartitions) {
                    if (id != so.aggr.swappingTarget) {
                        if (so.aggr.debug)
                            Rt.p(so.curPartition + "->" + so.aggr.targetPartition
                                    + " send swap to " + id + " " + swap);
                        so.sendObj(id, swap);
                    }
                }
                swap.newTargetPartition = so.curPartition;
                if (so.aggr.newChildren != null) {
                    for (int id : so.aggr.newChildren) {
                        if (so.aggr.debug)
                            Rt.p(so.curPartition + "->" + so.aggr.targetPartition
                                    + " send swap to " + id + " " + swap);
                        so.sendObj(id, swap);
                    }
                }
                if (so.aggr.debug)
                    Rt
                            .p(so.curPartition + "->" + so.aggr.targetPartition
                                    + " send swap to " + so.aggr.swappingTarget
                                    + " " + swap);
                swap.newTargetPartition = so.aggr.swappingTarget;
                swap.incompeleteIncomingPartitions = so.aggr.successfullyHoldPartitions;
                so.sendObj(so.aggr.swappingTarget, swap);
                so.aggr.targetPartition = so.aggr.swappingTarget;
                so.aggr.log.append("i" + so.aggr.targetPartition + ",");

                so.aggr.swapChildren(so.aggr.successfullyHoldPartitions, so.aggr.newChildren,
                        -1);
                if (target >= 0)
                    so.sendObj(target, new SwapChildrenRequest(so.curPartition,
                            so.aggr.swappingTarget));
                if (so.aggr.debug)
                    Rt.p("swap " + so.curPartition + " with "
                            + so.aggr.swappingTarget);
                so.aggr.swaps.add(so.aggr.swappingTarget);
                so.aggr.swapsTime.add(System.currentTimeMillis());
                so.aggr.swappingTarget = -1;
                break;
            }
        }
    }
}
