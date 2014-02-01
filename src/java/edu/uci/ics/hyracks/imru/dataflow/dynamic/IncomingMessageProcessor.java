package edu.uci.ics.hyracks.imru.dataflow.dynamic;

import java.io.IOException;
import java.util.Vector;

import edu.uci.ics.hyracks.imru.dataflow.dynamic.map.GetAvailableSplits;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.map.ReplyAvailableSplit;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.map.ReplySplit;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.map.RequestSplit;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.swap.IdentificationCorrection;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.swap.IdentifyRequest;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.swap.LockReply;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.swap.LockRequest;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.swap.ReleaseLock;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.swap.SwapChildrenRequest;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.swap.DynamicCommand;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.swap.SwapTargetRequest;
import edu.uci.ics.hyracks.imru.util.Rt;

public class IncomingMessageProcessor {
    ImruSendOperator so;
    int srcPartition;
    int thisPartition;
    int replyPartition;

    public IncomingMessageProcessor(ImruSendOperator sendOperator) {
        this.so = sendOperator;
    }

    public synchronized void recvObject(int srcPartition, int thisPartition,
            int replyPartition, Object object) throws IOException {
        this.srcPartition = srcPartition;
        this.thisPartition = thisPartition;
        this.replyPartition = replyPartition;
        if (so.aggr.targetPartition == -2)
            return;
        if (so.aggr.debug && object instanceof DynamicCommand)
            Rt.p(so.curPartition + " recv from " + srcPartition + " {" + object
                    + "}");
        if (object instanceof LockRequest) {
            processLockRequest((LockRequest) object);
        } else if (object instanceof LockReply) {
            processLockReply((LockReply) object);
        } else if (object instanceof SwapTargetRequest) {
            processSwapTargetRequest((SwapTargetRequest) object);
        } else if (object instanceof SwapChildrenRequest) {
            SwapChildrenRequest request = (SwapChildrenRequest) object;
            if (so.aggr.debug)
                Rt.p(so.curPartition + " recv swapChildren from "
                        + srcPartition + " " + request);
            if (request.addPartition == so.curPartition) {
                Rt.p("ERROR ");
                return;
            }
            if (so.aggr.isParentNodeOfSwapping
                    && request.removePartition == so.aggr.swappingTarget) {
                // The target partition swapped itself away
                so.aggr.swapFailed = true;
                so.aggr.failedReason = "disappear";
                synchronized (so.aggr.aggrSync) {
                    so.aggr.aggrSync.notifyAll();
                }
            }
            for (int i = 0; i < so.aggr.incomingPartitions.length; i++) {
                if (so.aggr.incomingPartitions[i] == request.removePartition) {
                    so.aggr.incomingPartitions[i] = request.addPartition;
                }
                synchronized (so.aggr.aggrSync) {
                    so.aggr.aggrSync.notifyAll();
                }
            }
        } else if (object instanceof ReleaseLock) {
            synchronized (so.aggr.aggrSync) {
                so.aggr.holding = false;
                so.aggr.aggrSync.notifyAll();
            }
        } else if (object instanceof IdentifyRequest) {
            IdentificationCorrection c = new IdentificationCorrection(
                    so.curPartition, thisPartition);
            for (int i = 0; i < so.nPartitions; i++) {
                so.sendObjToWriter(i, c);
            }
        } else if (object instanceof IdentificationCorrection) {
            IdentificationCorrection ic = (IdentificationCorrection) object;
            Rt.p("correct " + so.curPartition + " " + ic);
            so.partitionWriter[ic.partition] = ic.writer;
            synchronized (so.receivedIdentificationSync) {
                if (!so.receivedIdentifications.get(ic.partition)) {
                    so.receivedIdentifications.set(ic.partition);
                    so.receivedIdentificationCorrections++;
                    so.receivedIdentificationSync.notifyAll();
                }
            }
        } else if (object instanceof GetAvailableSplits
                || object instanceof ReplyAvailableSplit
                || object instanceof RequestSplit
                || object instanceof ReplySplit) {
            so.map.processIncomingMessage(srcPartition, thisPartition,
                    replyPartition, object);
        } else
            throw new Error();
    }

    void processLockRequest(LockRequest request) throws IOException {
        boolean successful;
        String reason = null;
        synchronized (so.aggr.aggrSync) {
            if (so.aggr.holding) {
                successful = false;
                reason = "hold";
            } else if (so.aggr.receivedMapResult) {
                successful = false;
                reason = "mapped";
            } else if (so.aggr.sending) {
                successful = false;
                reason = "send";
            } else if (so.aggr.swappingTarget >= 0) {
                successful = false;
                reason = "swapping";
            } else {
                so.aggr.holding = true;
                successful = true;
            }
        }
        if (request.isParentNode
                && so.curPartition == request.newTargetPartition) {
            if (successful)
                so.aggr.log.append("accept" + srcPartition + ",");
            else
                so.aggr.log.append("reject" + srcPartition + ",");
        }
        if (successful && request.isParentNode
                && so.curPartition == request.newTargetPartition) {
            // for the swap target
            int n = 0;
            if (so.aggr.incomingPartitions.length > 0)
                n = so.aggr.lockIncomingPartitions(false, -1);
            so.aggr.isParentNodeOfSwapping = false;
            so.aggr.swappingTarget = srcPartition;
            if (n == 0) {
                so.aggr.successfullyHoldPartitions = new int[0];
                LockReply r2 = new LockReply();
                r2.forParentNode = true;
                r2.successful = true;
                r2.holdedIncomingPartitions = new int[0];
                so.sendObj(srcPartition, r2);
            }
        } else {
            LockReply reply = new LockReply();
            reply.forParentNode = request.isParentNode;
            reply.successful = successful;
            reply.reason = reason;
            so.sendObj(srcPartition, reply);
            if (so.aggr.debug)
                Rt.p(so.curPartition + " reply " + srcPartition + " with "
                        + reply);
        }
    }

    void checkHoldingStatus() throws IOException {
        for (int i = so.aggr.expectingReplies.nextSetBit(0); i >= 0; i = so.aggr.expectingReplies
                .nextSetBit(i + 1)) {
            if (so.aggr.receivedPartitions.get(i)) {
                so.aggr.expectingReplies.clear(i);
                so.aggr.totalRepliesRemaining--;
            }
        }
    }

    void holdComplete() throws IOException {
        if (so.aggr.swapFailed) {
            synchronized (so.aggr.aggrSync) {
                so.aggr.aggrSync.notifyAll();
            }
            so.aggr.releaseIncomingPartitions();
        } else {
            Vector<Integer> succeed = new Vector<Integer>();
            for (int i : so.aggr.incomingPartitions)
                if (so.aggr.holdSucceed.get(i))
                    succeed.add(i);
            so.aggr.successfullyHoldPartitions = Rt.intArray(succeed);
            if (so.aggr.isParentNodeOfSwapping) {
                so.aggr.swapSucceed = true;
                if (so.aggr.debug)
                    Rt.p("*** GO " + so.curPartition + " swap with "
                            + so.aggr.swappingTarget + " go ahead");
                synchronized (so.aggr.aggrSync) {
                    so.aggr.aggrSync.notifyAll();
                }
            } else {
                LockReply r2 = new LockReply();
                r2.forParentNode = true;
                r2.successful = true;
                r2.holdedIncomingPartitions = so.aggr.successfullyHoldPartitions;
                if (so.aggr.swappingTarget < 0)
                    throw new Error();
                so.sendObj(so.aggr.swappingTarget, r2);
            }
        }
    }

    void processLockReply(LockReply reply) throws IOException {
        if (so.aggr.debug) {
            StringBuilder sb = new StringBuilder();
            for (int i = so.aggr.expectingReplies.nextSetBit(0); i >= 0; i = so.aggr.expectingReplies
                    .nextSetBit(i + 1)) {
                if (!so.aggr.receivedPartitions.get(i))
                    sb.append(i + ",");
            }
            Rt.p(so.curPartition + " recv holdReply from " + srcPartition + " "
                    + reply + " remaining " + so.aggr.totalRepliesRemaining
                    + " (" + sb + ")");

        }
        if (so.aggr.isParentNodeOfSwapping) {
            if (srcPartition == so.aggr.swappingTarget) {
                so.aggr.newChildren = reply.holdedIncomingPartitions;
            }
        }
        synchronized (so.aggr.aggrSync) {
            if (so.aggr.expectingReplies.get(srcPartition)) {
                so.aggr.expectingReplies.clear(srcPartition);
                so.aggr.totalRepliesRemaining--;
            }
            checkHoldingStatus();
        }
        so.aggr.holdSucceed.set(srcPartition, reply.successful);
        if (srcPartition == so.aggr.swappingTarget) {
            if (!reply.successful) {
                so.aggr.swapFailed = true;
                so.aggr.failedReason = reply.reason;
            }
        }
        int t = so.aggr.swappingTarget;
        if (t >= 0 && (so.aggr.isPartitionFinished(so.aggr.swappingTarget))) {
            //Already received from the target partition
            so.aggr.swapFailed = true;
            so.aggr.failedReason = "Recv" + so.aggr.failedReasonReceivedSize;
        }
        //            Rt.p(curPartition + " waiting reply for " + totalRepliesRemaining
        //                    + " more");
        if (so.aggr.totalRepliesRemaining <= 0)
            holdComplete();
    }

    void processSwapTargetRequest(SwapTargetRequest request) throws IOException {
        if (so.aggr.debug)
            Rt.p(so.curPartition + "->" + so.aggr.targetPartition
                    + " recv swap from " + srcPartition + " " + request);
        if (request.newTargetPartition == so.curPartition) {
            if (so.aggr.targetPartition != srcPartition) {
                so.aggr.printAggrTree();
                Rt.p("ERROR: " + so.curPartition + " "
                        + so.aggr.targetPartition + " " + srcPartition);
                //                throw new Error(so.targetPartition + " " + srcPartition);
            }
            if (so.aggr.swappingTarget != srcPartition) {
                so.aggr.printAggrTree();
                Rt.p("ERROR: " + so.curPartition + " "
                        + so.aggr.targetPartition + " " + srcPartition);
                //                throw new Error(so.targetPartition + " " + srcPartition);
            }
            so.aggr.targetPartition = request.outgoingPartitionOfSender;
            so.aggr.log.append("t" + so.aggr.targetPartition + ",");
            so.aggr.swapChildren(so.aggr.successfullyHoldPartitions,
                    request.incompeleteIncomingPartitions,
                    so.aggr.swappingTarget);
            so.aggr.swapped.add(so.aggr.swappingTarget);
            so.aggr.swappedTime.add(System.currentTimeMillis());
            if (so.aggr.isParentNodeOfSwapping)
                Rt.p("ERROR");
            else
                so.aggr.swappingTarget = -1;
            synchronized (so.aggr.aggrSync) {
                so.aggr.holding = false;
                so.aggr.aggrSync.notifyAll();
            }
            if (so.aggr.targetPartition >= 0)
                so.sendObj(so.aggr.targetPartition, new SwapChildrenRequest(
                        srcPartition, so.curPartition));
        } else {
            so.aggr.targetPartition = request.newTargetPartition;
            so.aggr.log.append(so.aggr.targetPartition + ",");
        }
        synchronized (so.aggr.aggrSync) {
            so.aggr.holding = false;
            so.aggr.aggrSync.notifyAll();
        }
    }
}
