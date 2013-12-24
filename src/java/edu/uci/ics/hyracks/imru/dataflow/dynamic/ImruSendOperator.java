package edu.uci.ics.hyracks.imru.dataflow.dynamic;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Vector;
import java.util.concurrent.Future;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.imru.api.ASyncIO;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api.IMRUReduceContext;
import edu.uci.ics.hyracks.imru.api.ImruFrames;
import edu.uci.ics.hyracks.imru.api.ImruParameters;
import edu.uci.ics.hyracks.imru.api.ImruStream;
import edu.uci.ics.hyracks.imru.api.old.IIMRUJob2;
import edu.uci.ics.hyracks.imru.data.MergedFrames;
import edu.uci.ics.hyracks.imru.data.SerializedFrames;
import edu.uci.ics.hyracks.imru.dataflow.IMRUSerialize;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUConnection;
import edu.uci.ics.hyracks.imru.util.Rt;

public class ImruSendOperator<Model extends Serializable, Data extends Serializable>
        extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
    boolean diableSwapping = false;
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

    final ImruFrames<Model, Data> imruSpec;
    ImruParameters parameters;
    IMRUReduceContext imruContext;
    String modelName;
    IMRUConnection imruConnection;
    Hashtable<Integer, LinkedList<ByteBuffer>> hash = new Hashtable<Integer, LinkedList<ByteBuffer>>();
    public String name;
    ASyncIO<byte[]> io;
    Future future;

    IHyracksTaskContext ctx;
    int curPartition;
    int nPartitions;
    IMRUContext context;
    BitSet receivingPartitions = new BitSet();
    BitSet receivedPartitions = new BitSet();

    Object aggrSync = new Object();
    int targetPartition;
    StringBuilder log = new StringBuilder();
    int sentPartition;
    int[] incomingPartitions;
    //    String aggrResult;
    boolean holding = false;
    boolean sending = false;
    int frameSize;
    ByteBuffer frame;

    //for swapping
    boolean swapSucceed;
    boolean swapFailed;
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

    DynamicAggregation<Model, Data> dynamicAggregation = new DynamicAggregation<Model, Data>(
            this);
    IncomingMessageProcessor incomingMessageProcessor = new IncomingMessageProcessor(
            this);

    public ImruSendOperator(IHyracksTaskContext ctx, int curPartition,
            int nPartitions, int[] targetPartitions,
            ImruStream<Model, Data> imruSpec, ImruParameters parameters,
            String modelName, IMRUConnection imruConnection,
            boolean diableSwapping, int maxWaitTimeBeforeSwap)
            throws HyracksDataException {
        this.ctx = ctx;
        this.curPartition = curPartition;
        this.nPartitions = nPartitions;
        this.imruSpec = (ImruFrames<Model, Data>)imruSpec;
        this.parameters = parameters;
        this.modelName = modelName;
        this.imruConnection = imruConnection;
        this.diableSwapping = diableSwapping;
        this.maxWaitTimeBeforeSwap = maxWaitTimeBeforeSwap;
        frameSize = ctx.getFrameSize();
        frame = ctx.allocateFrame();
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
    }

    void sendObj(int targetPartition, Serializable object) throws IOException {
        if (debug)
            Rt.p(curPartition + " send to " + targetPartition + " " + object);
        if (targetPartition < 0 || targetPartition >= nPartitions)
            throw new Error("" + targetPartition);
        synchronized (frame) {
            MergedFrames.serializeToFrames(frame, frameSize, writer, object,
                    curPartition, targetPartition);
        }
    }

    Vector<Integer> lockingPartitions;

    int lockIncomingPartitions(boolean isParentNode, int target)
            throws IOException {
        Vector<Integer> lockingPartitions = new Vector<Integer>();
        synchronized (aggrSync) {
            expectingReplies = new BitSet();
            for (int id : incomingPartitions) {
                if (!receivingPartitions.get(id)) {
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

    public void progress(int sourceParition, int targetParition, int size,
            int total, Object object) {
        //                        Rt.p(targetParition + " recv " + sourceParition + " "
        //                                + size + "/" + total);
        if (!(object instanceof SwapCommand))
            receivingPartitions.set(sourceParition);
    }

    public void complete(int srcPartition, int thisPartition,
            int replyPartition, Object object) throws IOException {
        incomingMessageProcessor.recvObject(srcPartition, thisPartition,
                replyPartition, object);
    }

    byte[] aggregatedResult;

    @Override
    public void open() throws HyracksDataException {
        this.name = "reduce " + curPartition;
        context = new IMRUContext(ctx, "send", curPartition);
        context.setUserObject("sendOperator", this);

        writer.open();

        imruContext = new IMRUReduceContext(ctx, name, false, -1, curPartition);
        io = new ASyncIO<byte[]>(1);
        future = IMRUSerialize.threadPool.submit(new Runnable() {
            @Override
            public void run() {
                Iterator<byte[]> input = io.getInput();
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                try {
                    imruSpec.reduceFrames(imruContext, input, out);
                    aggregatedResult = out.toByteArray();
                    //                    IMRUDebugger.sendDebugInfo(imruContext.getNodeId()
                    //                            + " reduce start " + curPartition);
                    if (imruContext.getIterationNumber() >= parameters.compressIntermediateResultsAfterNIterations)
                        aggregatedResult = IMRUSerialize
                                .compress(aggregatedResult);
                    synchronized (aggrSync) {
                        aggrSync.notifyAll();
                    }
                    //                    MergedFrames.serializeToFrames(imruContext, writer,
                    //                            objectData, curPartition, imruContext.getNodeId()
                    //                                    + " reduce " + curPartition + " "
                    //                                    + imruContext.getOperatorName());
                    //                    IMRUDebugger.sendDebugInfo(imruContext.getNodeId()
                    //                            + " reduce finish");
                } catch (Exception e) {
                    e.printStackTrace();
                    try {
                        fail();
                    } catch (HyracksDataException e1) {
                        e1.printStackTrace();
                    }
                }
            }
        });
    }

    @Override
    public void nextFrame(ByteBuffer encapsulatedChunk)
            throws HyracksDataException {
        try {
            MergedFrames frames = MergedFrames.nextFrame(ctx,
                    encapsulatedChunk, hash, imruContext.getNodeId() + " recv "
                            + curPartition + " "
                            + imruContext.getOperatorName());
            if (frames.data != null) {
                if (ImruSendOperator.debugNetworkSpeed > 0)
                    Thread
                            .sleep((int) (frames.data.length / ImruSendOperator.debugNetworkSpeed));

                if (imruContext.getIterationNumber() >= parameters.compressIntermediateResultsAfterNIterations)
                    frames.data = IMRUSerialize.decompress(frames.data);
                io.add(frames.data);
                if (debug) {
                    Rt.p(curPartition + " received map result from "
                            + frames.sourceParition + " "
                            + MergedFrames.deserialize(frames.data));
                }
            }
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