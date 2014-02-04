package edu.uci.ics.hyracks.imru.dataflow.dynamic;

import java.io.IOException;
import java.util.BitSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.uci.ics.hyracks.imru.dataflow.dynamic.map.GetAvailableSplits;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.map.ReplyAvailableSplit;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.map.ReplySplit;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.map.RequestSplit;
import edu.uci.ics.hyracks.imru.file.HDFSSplit;

public class MapStates {
    int parent;
    int[] children;
    int[] connected;
    BitSet depletedPartitions = new BitSet();
    int depletedCount = 0;
    Object queueSync = new Object();
    LinkedList<ReplyAvailableSplit> incomingQueue = new LinkedList<ReplyAvailableSplit>();

    BitSet relocatedSplits = new BitSet();
    BitSet deniedSplits = new BitSet();
    Object splitSync = new Object();
    ImruSendOperator so;
    LinkedList<HDFSSplit> queue;
    HashSet<Long> waitingRequests = new HashSet<Long>();

    public MapStates(ImruSendOperator so) {
        this.so = so;
        queue = so.imruContext.getQueue();
    }

    public void processIncomingMessage(int srcPartition, int thisPartition,
            int replyPartition, Object object) throws IOException {
        if (object instanceof GetAvailableSplits) {
            GetAvailableSplits get = (GetAvailableSplits) object;
            HDFSSplit split = null;
            synchronized (queue) {
                if (queue.size() > 0) {
                    split = queue.getLast();
                }
            }
            if (split != null) {
                ReplyAvailableSplit reply = new ReplyAvailableSplit(get,
                        so.curPartition, split.uuid);
                so.sendObj(srcPartition, reply);
            } else {
                waitingRequests.add(get.uuid);
                boolean sent = false;
                for (int p : connected) {
                    if (p != srcPartition && !depletedPartitions.get(p)) {
                        if (so.aggr.receivingPartitions.get(p))
                            depletedPartitions.set(p);
                        else {
                            GetAvailableSplits forword = get.clone();
                            forword.forwardedPartitions.add(so.curPartition);
                            so.sendObj(p, forword);
                            sent = true;
                        }
                    }
                }
                if (!sent) {
                    // no more partitions
                    ReplyAvailableSplit reply = new ReplyAvailableSplit(get,
                            so.curPartition, -1);
                    so.sendObj(srcPartition, reply);
                    waitingRequests.remove(get.uuid);
                }
            }
        } else if (object instanceof ReplyAvailableSplit) {
            ReplyAvailableSplit reply = (ReplyAvailableSplit) object;
            if (reply.splitUUID >= 0) {
                if (so.curPartition == reply.requestedBy) {
                    incomingQueue.add(reply);
                    synchronized (queueSync) {
                        queueSync.notifyAll();
                    }
                } else {
                    if (reply.forwardedPartitions.lastElement() != so.curPartition)
                        throw new Error();
                    reply.forwardedPartitions.remove(reply.forwardedPartitions
                            .size() - 1);
                    waitingRequests.remove(reply.uuid);
                    if (reply.forwardedPartitions.size() > 0) {
                        so.sendObj(reply.forwardedPartitions.lastElement(),
                                reply);
                    } else {
                        so.sendObj(reply.requestedBy, reply);
                    }
                }
            } else {
                //no more split from that direction
                synchronized (queueSync) {
                    if (!depletedPartitions.get(srcPartition)) {
                        depletedPartitions.set(srcPartition);
                        for (int p : connected) {
                            if (srcPartition == p) {
                                depletedCount++;
                                break;
                            }
                        }
                    }
                }
                if (so.curPartition == reply.requestedBy) {
                    synchronized (queueSync) {
                        queueSync.notifyAll();
                    }
                } else {
                    if (reply.forwardedPartitions.lastElement() != so.curPartition)
                        throw new Error();
                    reply.forwardedPartitions.remove(reply.forwardedPartitions
                            .size() - 1);
                    int src = reply.forwardedPartitions.size() > 0 ? reply.forwardedPartitions
                            .lastElement()
                            : reply.requestedBy;
                    if (waitingRequests.contains(reply.uuid)) {
                        boolean allDepleted = true;
                        for (int p : connected) {
                            if (p != src && !depletedPartitions.get(p)) {
                                allDepleted = false;
                                break;
                            }
                        }
                        if (allDepleted)
                            so.sendObj(src, reply);
                    }
                }
            }
        } else if (object instanceof RequestSplit) {
            RequestSplit request = (RequestSplit) object;
            if (request.splitLocation != so.curPartition)
                throw new Error();
            HDFSSplit split = so.imruContext
                    .removeSplitFromQueue(request.splitUUID);
            ReplySplit reply = new ReplySplit(request.requestedBy,
                    request.splitLocation, request.splitUUID, split != null);
            so.sendObj(reply.requestedBy, reply);
        } else if (object instanceof ReplySplit) {
            ReplySplit reply = (ReplySplit) object;
            if (reply.relocate)
                relocatedSplits.set(reply.splitUUID);
            else
                deniedSplits.set(reply.splitUUID);
            synchronized (splitSync) {
                splitSync.notifyAll();
            }
        }
    }
}
