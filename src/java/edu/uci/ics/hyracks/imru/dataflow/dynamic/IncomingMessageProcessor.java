package edu.uci.ics.hyracks.imru.dataflow.dynamic;

import java.io.IOException;
import java.util.Vector;

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
        if (so.targetPartition == -2)
            return;
        if (so.debug && object instanceof SwapCommand)
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
            if (so.debug)
                Rt.p(so.curPartition + " recv swapChildren from "
                        + srcPartition + " " + request);
            if (request.addPartition == so.curPartition) {
                Rt.p("ERROR ");
                return;
            }
            if (so.isParentNodeOfSwapping
                    && request.removePartition == so.swappingTarget) {
                // The target partition swapped itself away
                so.swapFailed = true;
                so.failedReason = "disappear";
                synchronized (so.aggrSync) {
                    so.aggrSync.notifyAll();
                }
            }
            for (int i = 0; i < so.incomingPartitions.length; i++) {
                if (so.incomingPartitions[i] == request.removePartition) {
                    so.incomingPartitions[i] = request.addPartition;
                }
                synchronized (so.aggrSync) {
                    so.aggrSync.notifyAll();
                }
            }
        } else if (object instanceof ReleaseLock) {
            synchronized (so.aggrSync) {
                so.holding = false;
                so.aggrSync.notifyAll();
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
        } else
            throw new Error();
    }

    void processLockRequest(LockRequest request) throws IOException {
        boolean successful;
        String reason = null;
        synchronized (so.aggrSync) {
            if (so.holding) {
                successful = false;
                reason = "hold";
            } else if (so.receivedMapResult) {
                successful = false;
                reason = "mapped";
            } else if (so.sending) {
                successful = false;
                reason = "send";
            } else if (so.swappingTarget >= 0) {
                successful = false;
                reason = "swapping";
            } else {
                so.holding = true;
                successful = true;
            }
        }
        if (request.isParentNode
                && so.curPartition == request.newTargetPartition) {
            if (successful)
                so.log.append("accept" + srcPartition + ",");
            else
                so.log.append("reject" + srcPartition + ",");
        }
        if (successful && request.isParentNode
                && so.curPartition == request.newTargetPartition) {
            // for the swap target
            int n = 0;
            if (so.incomingPartitions.length > 0)
                n = so.lockIncomingPartitions(false, -1);
            so.isParentNodeOfSwapping = false;
            so.swappingTarget = srcPartition;
            if (n == 0) {
                so.successfullyHoldPartitions = new int[0];
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
            if (so.debug)
                Rt.p(so.curPartition + " reply " + srcPartition + " with "
                        + reply);
        }
    }

    void checkHoldingStatus() throws IOException {
        for (int i = so.expectingReplies.nextSetBit(0); i >= 0; i = so.expectingReplies
                .nextSetBit(i + 1)) {
            if (so.receivedPartitions.get(i)) {
                so.expectingReplies.clear(i);
                so.totalRepliesRemaining--;
            }
        }
    }

    void holdComplete() throws IOException {
        if (so.swapFailed) {
            synchronized (so.aggrSync) {
                so.aggrSync.notifyAll();
            }
            so.releaseIncomingPartitions();
        } else {
            Vector<Integer> succeed = new Vector<Integer>();
            for (int i : so.incomingPartitions)
                if (so.holdSucceed.get(i))
                    succeed.add(i);
            so.successfullyHoldPartitions = Rt.intArray(succeed);
            if (so.isParentNodeOfSwapping) {
                so.swapSucceed = true;
                if (so.debug)
                    Rt.p("*** GO " + so.curPartition + " swap with "
                            + so.swappingTarget + " go ahead");
                synchronized (so.aggrSync) {
                    so.aggrSync.notifyAll();
                }
            } else {
                LockReply r2 = new LockReply();
                r2.forParentNode = true;
                r2.successful = true;
                r2.holdedIncomingPartitions = so.successfullyHoldPartitions;
                if (so.swappingTarget < 0)
                    throw new Error();
                so.sendObj(so.swappingTarget, r2);
            }
        }
    }

    void processLockReply(LockReply reply) throws IOException {
        if (so.debug) {
            StringBuilder sb = new StringBuilder();
            for (int i = so.expectingReplies.nextSetBit(0); i >= 0; i = so.expectingReplies
                    .nextSetBit(i + 1)) {
                if (!so.receivedPartitions.get(i))
                    sb.append(i + ",");
            }
            Rt.p(so.curPartition + " recv holdReply from " + srcPartition + " "
                    + reply + " remaining " + so.totalRepliesRemaining + " ("
                    + sb + ")");

        }
        if (so.isParentNodeOfSwapping) {
            if (srcPartition == so.swappingTarget) {
                so.newChildren = reply.holdedIncomingPartitions;
            }
        }
        synchronized (so.aggrSync) {
            if (so.expectingReplies.get(srcPartition)) {
                so.expectingReplies.clear(srcPartition);
                so.totalRepliesRemaining--;
            }
            checkHoldingStatus();
        }
        so.holdSucceed.set(srcPartition, reply.successful);
        if (srcPartition == so.swappingTarget) {
            if (!reply.successful) {
                so.swapFailed = true;
                so.failedReason = reply.reason;
            }
        }
        int t = so.swappingTarget;
        if (t >= 0 && (so.isPartitionFinished(so.swappingTarget))) {
            //Already received from the target partition
            so.swapFailed = true;
            so.failedReason = "Recv" + so.failedReasonReceivedSize;
        }
        //            Rt.p(curPartition + " waiting reply for " + totalRepliesRemaining
        //                    + " more");
        if (so.totalRepliesRemaining <= 0)
            holdComplete();
    }

    void processSwapTargetRequest(SwapTargetRequest request) throws IOException {
        if (so.debug)
            Rt.p(so.curPartition + "->" + so.targetPartition
                    + " recv swap from " + srcPartition + " " + request);
        if (request.newTargetPartition == so.curPartition) {
            if (so.targetPartition != srcPartition) {
                so.printAggrTree();
                Rt.p("ERROR: " + so.curPartition + " " + so.targetPartition
                        + " " + srcPartition);
                //                throw new Error(so.targetPartition + " " + srcPartition);
            }
            if (so.swappingTarget != srcPartition) {
                so.printAggrTree();
                Rt.p("ERROR: " + so.curPartition + " " + so.targetPartition
                        + " " + srcPartition);
                //                throw new Error(so.targetPartition + " " + srcPartition);
            }
            so.targetPartition = request.outgoingPartitionOfSender;
            so.log.append("t" + so.targetPartition + ",");
            so.swapChildren(so.successfullyHoldPartitions,
                    request.incompeleteIncomingPartitions, so.swappingTarget);
            so.swapped.add(so.swappingTarget);
            so.swappedTime.add(System.currentTimeMillis());
            if (so.isParentNodeOfSwapping)
                Rt.p("ERROR");
            else
                so.swappingTarget = -1;
            synchronized (so.aggrSync) {
                so.holding = false;
                so.aggrSync.notifyAll();
            }
            if (so.targetPartition >= 0)
                so.sendObj(so.targetPartition, new SwapChildrenRequest(
                        srcPartition, so.curPartition));
        } else {
            so.targetPartition = request.newTargetPartition;
            so.log.append(so.targetPartition + ",");
        }
        synchronized (so.aggrSync) {
            so.holding = false;
            so.aggrSync.notifyAll();
        }
    }
}
