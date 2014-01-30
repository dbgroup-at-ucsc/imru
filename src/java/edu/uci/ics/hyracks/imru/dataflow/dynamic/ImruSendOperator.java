package edu.uci.ics.hyracks.imru.dataflow.dynamic;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Stack;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.imru.api.IMRUReduceContext;
import edu.uci.ics.hyracks.imru.api.ImruFrames;
import edu.uci.ics.hyracks.imru.api.ImruParameters;
import edu.uci.ics.hyracks.imru.api.ImruStream;
import edu.uci.ics.hyracks.imru.data.SerializedFrames;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUConnection;
import edu.uci.ics.hyracks.imru.util.Rt;

public class ImruSendOperator<Model extends Serializable, Data extends Serializable>
        extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
    boolean disableSwapping = false;
    int maxWaitTimeBeforeSwap = 1000;
    public static boolean debug = false;
    public static int debugNetworkSpeed = 0;
    public static int debugNodeCount = 8;
    public static ImruSendOperator[] debugSendOperators = new ImruSendOperator[100];

    static void printAggrTree() {
        if (!debug)
            return;
        StringBuilder sb = new StringBuilder();
        sb.append("Current aggregation tree\n");
        for (int i = 0; i < debugNodeCount; i++) {
            ImruSendOperator o = debugSendOperators[i];
            if (o == null)
                continue;
            if (o.targetPartition < -1) {
                sb.append(i + ":\t sent to " + o.sentPartition + "\t(" + o.log
                        + ")\n");
                continue;
            }
            sb.append(i + ":\t(");
            for (int j : o.incomingPartitions) {
                if (o.receivingPartitions.get(j))
                    sb.append("*");
                sb.append(j + ",");
            }
            sb.append(") -> " + i + " -> " + o.targetPartition);
            if (o.receivedMapResult)
                sb.append(" map");
            if (o.allChildrenFinished)
                sb.append(" ready");
            if (o.holding)
                sb.append(" hold");
            if (o.sending)
                sb.append(" sending");
            if (o.swappingTarget >= 0) {
                sb.append(" swapTo" + o.swappingTarget);
                if (o.isParentNodeOfSwapping)
                    sb.append("i");
            }
            sb.append("\t(" + o.log + ")\n");
        }
        synchronized (debugSendOperators) {
            Rt.np(sb);
        }
    }

    final ImruStream<Model, Data> imruSpec;
    ImruParameters parameters;
    IMRUReduceContext imruContext;
    String modelName;
    IMRUConnection imruConnection;
    Hashtable<Integer, LinkedList<ByteBuffer>> hash = new Hashtable<Integer, LinkedList<ByteBuffer>>();
    public String name;
    //    ASyncIO<byte[]> io;
    //    Future future;

    IHyracksTaskContext ctx;
    int curPartition;
    int nPartitions;

    //    IMRUContext context;
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

    DynamicAggregation<Model, Data> dynamicAggregation = new DynamicAggregation<Model, Data>(
            this);
    IncomingMessageProcessor incomingMessageProcessor = new IncomingMessageProcessor(
            this);
    int[] partitionWriter; //Mapping between partition and writer
    BitSet receivedIdentifications = new BitSet();
    int receivedIdentificationCorrections = 0;
    Object receivedIdentificationSync = new Object();

    public ImruSendOperator(IHyracksTaskContext ctx, int curPartition,
            int nPartitions, int[] targetPartitions,
            ImruStream<Model, Data> imruSpec, ImruParameters parameters,
            String modelName, IMRUConnection imruConnection,
            boolean diableSwapping, int maxWaitTimeBeforeSwap)
            throws HyracksDataException {
        this.ctx = ctx;
        this.curPartition = curPartition;
        this.nPartitions = nPartitions;
        this.imruSpec = (ImruFrames<Model, Data>) imruSpec;
        this.parameters = parameters;
        this.modelName = modelName;
        this.imruConnection = imruConnection;
        this.disableSwapping = diableSwapping;
        this.maxWaitTimeBeforeSwap = maxWaitTimeBeforeSwap;
        debugSendOperators[curPartition] = this;
        targetPartition = targetPartitions[curPartition];
        this.log.append(targetPartition + ",");
        int sourceCount = 0;
        for (int i : targetPartitions)
            if (i == curPartition)
                sourceCount++;
        incomingPartitions = new int[sourceCount];
        sourceCount = 0;
        for (int i = 0; i < targetPartitions.length; i++)
            if (targetPartitions[i] == curPartition)
                incomingPartitions[sourceCount++] = i;
        partitionWriter = new int[nPartitions];
        for (int i = 0; i < nPartitions; i++)
            partitionWriter[i] = i;
    }

    int partitionToWriter(int partitionId) {
        return partitionWriter[partitionId];
    }

    void sendObjToWriter(int targetPartition, SwapCommand cmd)
            throws IOException {
        if (debug)
            Rt.p(curPartition + " send to " + targetPartition + " " + cmd);
        if (targetPartition < 0 || targetPartition >= nPartitions)
            throw new Error("" + targetPartition);
        //        if (partitionToWriter(targetPartition) != targetPartition) {
        //            Rt.p("correct address: " + targetPartition + " -> "
        //                    + partitionToWriter(targetPartition));
        //        }
        SerializedFrames.serializeSwapCmd(imruContext, writer, cmd,
                curPartition, targetPartition, targetPartition);
    }

    void sendObj(int targetPartition, SwapCommand cmd) throws IOException {
        if (debug)
            Rt.p(curPartition + " send to " + targetPartition + " " + cmd);
        if (targetPartition < 0 || targetPartition >= nPartitions)
            throw new Error("" + targetPartition);
        //        if (partitionToWriter(targetPartition) != targetPartition) {
        //            Rt.p("correct address: " + targetPartition + " -> "
        //                    + partitionToWriter(targetPartition));
        //        }
        SerializedFrames.serializeSwapCmd(imruContext, writer, cmd,
                curPartition, targetPartition,
                partitionToWriter(targetPartition));
    }

    IFrameWriter getWriter() {
        return writer;
    }

    //    void sendData(int targetPartition, byte[] data) throws IOException {
    //        if (debug)
    //            Rt.p(curPartition + " send to " + targetPartition + " "
    //                    + data.length);
    //        if (targetPartition < 0 || targetPartition >= nPartitions)
    //            throw new Error("" + targetPartition);
    //        synchronized (frame) {
    //            SerializedFrames.serializeToFrames(null, frame, frameSize, writer,
    //                    data, curPartition, targetPartition, curPartition, null);
    //        }
    //    }

    Vector<Integer> lockingPartitions;

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
            if (id < 0 || id >= nPartitions)
                throw new Error("" + id);
            sendObj(id, r2);
        }
        this.lockingPartitions = lockingPartitions;
        return lockingPartitions.size();
    }

    void releaseIncomingPartitions() throws IOException {
        if (lockingPartitions == null)
            return;
        ReleaseLock r2 = new ReleaseLock();
        for (int id : lockingPartitions) {
            sendObj(id, r2);
        }
        lockingPartitions = null;
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
                if (i != curPartition)
                    v.add(i);
            }
        }
        incomingPartitions = Rt.intArray(v);
        if (debug)
            Rt.p("*** SWAP " + curPartition + "'s children: " + old + " -> "
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

    public void complete(int srcPartition, int thisPartition,
            int replyPartition, Object object) throws IOException {
        incomingMessageProcessor.recvObject(srcPartition, thisPartition,
                replyPartition, object);
    }

    public void completedAggr(int srcPartition, int thisPartition,
            int replyPartition) throws IOException {
        ImruSendOperator so = this;
        //        byte[] receivedResult = (byte[]) object;
        if (so.allChildrenFinished) {
            Rt.p("ERROR " + so.curPartition + " recv data from " + srcPartition
                    + " {"
                    //                    + MergedFrames.deserialize(receivedResult)
                    + "}");
        }
        synchronized (so.aggrSync) {
            //                String orgResult = aggrResult;
            //                addResult(receivedResult);
            //            so.io.add(receivedResult);
            if (so.receivedPartitions.get(srcPartition)) {
                new Error().printStackTrace();
            }
            so.receivedPartitions.set(srcPartition);
            //                if (debug)
            //                    Rt.p(context.getNodeId() + " received result from "
            //                            + object + ", " + orgResult + " + " + object
            //                            + " = " + aggrResult);
            so.aggrSync.notifyAll();
        }
        if (so.debug) {
            Rt.p(so.curPartition + " recv data from " + srcPartition + " {"
            //                    + MergedFrames.deserialize(receivedResult) 
                    + "}");
            so.printAggrTree();
        }
        if (so.totalRepliesRemaining > 0) {
            if (so.isParentNodeOfSwapping && srcPartition == so.swappingTarget) {
                //The target partition sent everything
                so.swapFailed = true;
                so.failedReason = "complete";
            }
            incomingMessageProcessor.checkHoldingStatus();
            if (so.totalRepliesRemaining <= 0)
                incomingMessageProcessor.holdComplete();
        }
    }

    Object dbgInfoRecvQueue;
    Object recvQueue;
    byte[] aggregatedResult;

    @Override
    public void open() throws HyracksDataException {
        this.name = "DR" + curPartition;
        imruContext = new IMRUReduceContext(ctx, name, false, -1, curPartition,
                nPartitions);
        imruContext.setUserObject("sendOperator", this);

        writer.open();

        recvQueue = imruSpec.reduceInit(imruContext, new OutputStream() {
            ByteArrayOutputStream out = new ByteArrayOutputStream();

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                out.write(b, off, len);
            }

            @Override
            public void write(int b) throws IOException {
                out.write(b);
            }

            @Override
            public void close() throws IOException {
                aggregatedResult = out.toByteArray();
                // if (imruContext.getIterationNumber() >=
                // parameters.compressIntermediateResultsAfterNIterations)
                // objectData = IMRUSerialize.compress(objectData);
                synchronized (aggrSync) {
                    aggrSync.notifyAll();
                }
                //                IMRUDebugger.sendDebugInfo(imruContext.getNodeId()
                //                        + " reduce finish");
            }
        });
        dbgInfoRecvQueue = imruSpec.reduceDbgInfoInit(imruContext, recvQueue);
        if (curPartition == 0) {
            // Find out the relationship between writers and partitions.
            for (int i = 0; i < nPartitions; i++) {
                try {
                    sendObjToWriter(i, new IdentifyRequest(curPartition, i));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        if (true) {
            long startTime = System.currentTimeMillis();
            while (true) {
                if (receivedIdentificationCorrections >= nPartitions)
                    break;
                try {
                    synchronized (receivedIdentificationSync) {
                        receivedIdentificationSync.wait(1000);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
                if (System.currentTimeMillis() - startTime > 10000) {
                    Rt
                            .p(this.curPartition
                                    + " still waiting for "
                                    + (nPartitions - receivedIdentificationCorrections));
                }
            }
            //            Rt.p(this.curPartition + " ready to go");
        }
        //        io = new ASyncIO<byte[]>(1);
        //        future = IMRUSerialize.threadPool.submit(new Runnable() {
        //            @Override
        //            public void run() {
        //                Iterator<byte[]> input = io.getInput();
        //                ByteArrayOutputStream out = new ByteArrayOutputStream();
        //                try {
        //                    imruSpec.reduceFrames(imruContext, input, out);
        //                    aggregatedResult = out.toByteArray();
        //                    //                    IMRUDebugger.sendDebugInfo(imruContext.getNodeId()
        //                    //                            + " reduce start " + curPartition);
        //                    if (imruContext.getIterationNumber() >= parameters.compressIntermediateResultsAfterNIterations)
        //                        aggregatedResult = IMRUSerialize
        //                                .compress(aggregatedResult);
        //                    synchronized (aggrSync) {
        //                        aggrSync.notifyAll();
        //                    }
        //                    //                    MergedFrames.serializeToFrames(imruContext, writer,
        //                    //                            objectData, curPartition, imruContext.getNodeId()
        //                    //                                    + " reduce " + curPartition + " "
        //                    //                                    + imruContext.getOperatorName());
        //                    //                    IMRUDebugger.sendDebugInfo(imruContext.getNodeId()
        //                    //                            + " reduce finish");
        //                } catch (Exception e) {
        //                    e.printStackTrace();
        //                    try {
        //                        fail();
        //                    } catch (HyracksDataException e1) {
        //                        e1.printStackTrace();
        //                    }
        //                }
        //            }
        //        });
    }

    @Override
    public void nextFrame(ByteBuffer encapsulatedChunk)
            throws HyracksDataException {
        try {
            SerializedFrames f = SerializedFrames.nextFrame(ctx.getFrameSize(),
                    encapsulatedChunk);
            if (f.replyPartition == SerializedFrames.DBG_INFO_FRAME)
                imruSpec.reduceDbgInfoReceive(f.srcPartition, f.offset,
                        f.totalSize, f.data, dbgInfoRecvQueue);
            else
                imruSpec.reduceReceive(f.srcPartition, f.offset, f.totalSize,
                        f.data, recvQueue);
            //            MergedFrames frames = MergedFrames.nextFrame(ctx,
            //                    encapsulatedChunk, hash, imruContext.getNodeId() + " recv "
            //                            + curPartition + " "
            //                            + imruContext.getOperatorName());
            //            if (frames.data != null) {
            //                if (ImruSendOperator.debugNetworkSpeed > 0)
            //                    Thread
            //                            .sleep((int) (frames.data.length / ImruSendOperator.debugNetworkSpeed));

            //                if (imruContext.getIterationNumber() >= parameters.compressIntermediateResultsAfterNIterations)
            //                    frames.data = IMRUSerialize.decompress(frames.data);
            //                io.add(frames.data);
            //                if (debug) {
            //                    Rt.p(curPartition + " received map result from "
            //                            + frames.sourceParition + " "
            //                            + MergedFrames.deserialize(frames.data));
            //                }
            //            }
        } catch (HyracksDataException e) {
            fail();
            throw e;
        } catch (Throwable e) {
            fail();
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void fail() throws HyracksDataException {

    }

    @Override
    public void close() throws HyracksDataException {
        //        Rt.p(curPartition + " close");
        try {
            boolean isRoot = dynamicAggregation.waitForAggregation();
            if (!isRoot)
                Thread.sleep(1000);
        } catch (Exception e) {
            writer.fail();
            throw new HyracksDataException(e);
        } finally {
            //            Rt.p("close "+ curPartition);
            writer.close();
        }
    }
}