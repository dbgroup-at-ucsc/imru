package exp.imruVsSpark.kmeans.exp.dynamic;

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
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.imru.api.ASyncIO;
import edu.uci.ics.hyracks.imru.api.IIMRUJob2;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api.IMRUReduceContext;
import edu.uci.ics.hyracks.imru.api.ImruIterationInformation;
import edu.uci.ics.hyracks.imru.api.ImruParameters;
import edu.uci.ics.hyracks.imru.data.MergedFrames;
import edu.uci.ics.hyracks.imru.dataflow.IMRUDebugger;
import edu.uci.ics.hyracks.imru.dataflow.IMRUSerialize;
import edu.uci.ics.hyracks.imru.dataflow.ReduceOperatorDescriptor;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUConnection;
import edu.uci.ics.hyracks.imru.util.Rt;

public class ImruSendOperator<Model extends Serializable, Data extends Serializable>
        extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
    public static boolean fixedTree = false;
    static boolean debug = false;
    public static ImruSendOperator[] debugSendOperators = new ImruSendOperator[100];

    private static void printAggrTree() {
        StringBuilder sb = new StringBuilder();
        sb.append("Current aggregation tree\n");
        for (int i = 0; i < 5; i++) {
            ImruSendOperator o = debugSendOperators[i];
            if (o.targetPartition < -1)
                continue;
            sb.append("(");
            for (int j : o.incomingPartitions) {
                if (o.receivingPartitions.get(j))
                    sb.append("*");
                sb.append(j + ",");
            }
            sb.append(") -> " + i + " -> " + o.targetPartition + "\n");
        }
        synchronized (debugSendOperators) {
            Rt.np(sb);
        }
    }

    private static String map(String nodeId, int curPartition) {
        Rt.sleep(100);
        //                        Rt.sleep(curPartition * 1000);
        return nodeId;
    }

    private static String aggregate(String a, String b) {
        return "(" + a + "," + b + ")";
    }

    private final IIMRUJob2<Model, Data> imruSpec;
    ImruParameters parameters;
    IMRUReduceContext imruContext;
    String modelName;
    IMRUConnection imruConnection;
    Hashtable<Integer, LinkedList<ByteBuffer>> hash = new Hashtable<Integer, LinkedList<ByteBuffer>>();
    public String name;
    private ASyncIO<byte[]> io;
    Future future;

    IHyracksTaskContext ctx;
    int curPartition;
    int nPartitions;
    IMRUContext context;
    BitSet receivingPartitions = new BitSet();
    BitSet receivedPartitions = new BitSet();

    Object aggrSync = new Object();
    int targetPartition;
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

    public ImruSendOperator(IHyracksTaskContext ctx, int curPartition,
            int nPartitions, int[] targetPartitions,
            IIMRUJob2<Model, Data> imruSpec, ImruParameters parameters,
            String modelName, IMRUConnection imruConnection)
            throws HyracksDataException {
        this.ctx = ctx;
        this.curPartition = curPartition;
        this.nPartitions = nPartitions;
        this.imruSpec = imruSpec;
        this.parameters = parameters;
        this.modelName = modelName;
        this.imruConnection = imruConnection;
        frameSize = ctx.getFrameSize();
        frame = ctx.allocateFrame();
        debugSendOperators[curPartition] = this;
        targetPartition = targetPartitions[curPartition];
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

    private void sendObj(int targetPartition, Serializable object)
            throws IOException {
        if (targetPartition < 0 || targetPartition >= nPartitions)
            throw new Error("" + targetPartition);
        synchronized (frame) {
            MergedFrames.serializeToFrames(frame, frameSize, writer, object,
                    curPartition, targetPartition);
        }
    }

    //    private void addResult(String result) {
    //        if (aggrResult == null)
    //            aggrResult = result;
    //        else
    //            aggrResult = aggregate(aggrResult, result);
    //    }

    private int lockIncomingPartitions(boolean isParentNode, int target)
            throws IOException {
        expectingReplies = new BitSet();
        Vector<Integer> unfinishedChildren = new Vector<Integer>();
        for (int id : incomingPartitions) {
            if (!receivingPartitions.get(id)) {
                expectingReplies.set(id);
                unfinishedChildren.add(id);
            }
        }
        totalRepliesRemaining = unfinishedChildren.size();
        LockRequest r2 = new LockRequest();
        r2.isParentNode = isParentNode;
        r2.newTargetPartition = target;
        for (int id : unfinishedChildren) {
            if (id < 0 || id >= nPartitions)
                throw new Error("" + id);
            sendObj(id, r2);
        }
        return unfinishedChildren.size();
    }

    private void releaseIncomingPartitions() throws IOException {
        ReleaseLock r2 = new ReleaseLock();
        for (int id : incomingPartitions) {
            if (!receivingPartitions.get(id)) {
                sendObj(id, r2);
            }
        }
    }

    private String getIncomingPartitionsString() {
        StringBuilder sb = new StringBuilder();
        for (int i : incomingPartitions)
            sb.append(i + ",");
        return sb.toString();
    }

    private void swapChildren(int[] remove, int[] add, int add2) {
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
        if (add2 >= 0)
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
        if (targetPartition == -2)
            return;
        if (debug && object instanceof SwapCommand)
            Rt.p(curPartition + " recv from " + srcPartition + " {" + object
                    + "}");
        if (object instanceof LockRequest) {
            LockRequest request = (LockRequest) object;
            boolean successful;
            synchronized (aggrSync) {
                if (sending || swappingTarget >= 0) {
                    successful = false;
                } else {
                    holding = true;
                    successful = true;
                }
            }
            if (successful && request.isParentNode
                    && curPartition == request.newTargetPartition) {
                // for the swap target
                int n = 0;
                if (incomingPartitions.length > 0)
                    n = lockIncomingPartitions(false, -1);
                isParentNodeOfSwapping = false;
                swappingTarget = srcPartition;
                if (n == 0) {
                    successfullyHoldPartitions = new int[0];
                    LockReply r2 = new LockReply();
                    r2.forParentNode = true;
                    r2.successful = true;
                    r2.holdedIncomingPartitions = new int[0];
                    sendObj(srcPartition, r2);
                }
            } else {
                LockReply reply = new LockReply();
                reply.forParentNode = request.isParentNode;
                reply.successful = successful;
                sendObj(srcPartition, reply);
                if (debug)
                    Rt.p(curPartition + " reply " + srcPartition + " with "
                            + reply);
            }
        } else if (object instanceof LockReply) {
            LockReply reply = (LockReply) object;
            if (debug) {
                StringBuilder sb = new StringBuilder();
                for (int i = expectingReplies.nextSetBit(0); i >= 0; i = expectingReplies
                        .nextSetBit(i + 1)) {
                    if (!receivedPartitions.get(i))
                        sb.append(i + ",");
                }
                Rt.p(curPartition + " recv holdReply from " + srcPartition
                        + " " + reply + " remaining " + totalRepliesRemaining
                        + " (" + sb + ")");

            }
            if (isParentNodeOfSwapping) {
                if (srcPartition == swappingTarget) {
                    newChildren = reply.holdedIncomingPartitions;
                }
            }
            synchronized (aggrSync) {
                if (expectingReplies.get(srcPartition)) {
                    expectingReplies.clear(srcPartition);
                    totalRepliesRemaining--;
                }
                for (int i = expectingReplies.nextSetBit(0); i >= 0; i = expectingReplies
                        .nextSetBit(i + 1)) {
                    if (receivedPartitions.get(i)) {
                        expectingReplies.clear(i);
                        totalRepliesRemaining--;
                    }
                }
            }
            holdSucceed.set(srcPartition, reply.successful);
            if (srcPartition == swappingTarget) {
                if (!reply.successful) {
                    swapFailed = true;
                }
            }
            //            Rt.p(curPartition + " waiting reply for " + totalRepliesRemaining
            //                    + " more");
            if (totalRepliesRemaining <= 0) {
                if (swapFailed) {
                    synchronized (aggrSync) {
                        aggrSync.notifyAll();
                    }
                    releaseIncomingPartitions();
                } else {
                    Vector<Integer> succeed = new Vector<Integer>();
                    for (int i : incomingPartitions)
                        if (holdSucceed.get(i))
                            succeed.add(i);
                    successfullyHoldPartitions = Rt.intArray(succeed);
                    if (isParentNodeOfSwapping) {
                        swapSucceed = true;
                        if (debug)
                            Rt.p("*** GO " + curPartition + " swap with "
                                    + swappingTarget + " go ahead");
                        synchronized (aggrSync) {
                            aggrSync.notifyAll();
                        }
                    } else {
                        LockReply r2 = new LockReply();
                        r2.forParentNode = true;
                        r2.successful = true;
                        r2.holdedIncomingPartitions = successfullyHoldPartitions;
                        if (swappingTarget < 0)
                            throw new Error();
                        sendObj(swappingTarget, r2);
                    }
                }
            }
        } else if (object instanceof SwapTargetRequest) {
            SwapTargetRequest request = (SwapTargetRequest) object;
            if (debug)
                Rt.p(curPartition + "->" + targetPartition + " recv swap from "
                        + srcPartition + " " + request);
            if (request.newTargetPartition == curPartition) {
                if (targetPartition != srcPartition) {
                    printAggrTree();
                    Rt.p(curPartition);
                    throw new Error(targetPartition + " " + srcPartition);
                }
                targetPartition = request.outgoingPartitionOfSender;
                swapChildren(successfullyHoldPartitions,
                        request.incompeleteIncomingPartitions, swappingTarget);
                swappingTarget = -1;
                synchronized (aggrSync) {
                    holding = false;
                    aggrSync.notifyAll();
                }
            } else {
                targetPartition = request.newTargetPartition;
            }
            synchronized (aggrSync) {
                holding = false;
                aggrSync.notifyAll();
            }
        } else if (object instanceof SwapChildrenRequest) {
            SwapChildrenRequest request = (SwapChildrenRequest) object;
            if (debug)
                Rt.p(curPartition + " recv swapChildren from " + srcPartition
                        + " " + request);
            for (int i = 0; i < incomingPartitions.length; i++) {
                if (incomingPartitions[i] == request.removePartition) {
                    incomingPartitions[i] = request.addPartition;
                }
            }
        } else if (object instanceof ReleaseLock) {
            synchronized (aggrSync) {
                holding = false;
                aggrSync.notifyAll();
            }
        } else {
            byte[] receivedResult = (byte[]) object;
            synchronized (aggrSync) {
                //                String orgResult = aggrResult;
                //                addResult(receivedResult);
                io.add(receivedResult);
                receivedPartitions.set(srcPartition);
                //                if (debug)
                //                    Rt.p(context.getNodeId() + " received result from "
                //                            + object + ", " + orgResult + " + " + object
                //                            + " = " + aggrResult);
                aggrSync.notifyAll();
            }
            if (debug) {
                Rt.p(curPartition + " recv data from " + srcPartition + " {"
                        + MergedFrames.deserialize(receivedResult) + "}");
                printAggrTree();
            }
        }
    }

    byte[] aggregatedResult;

    @Override
    public void open() throws HyracksDataException {
        this.name = "reduce " + curPartition;
        context = new IMRUContext(ctx, "send");
        context.setUserObject("aggrRecvCallback", this);

        writer.open();

        imruContext = new IMRUReduceContext(ctx, name, false, -1);
        io = new ASyncIO<byte[]>(1);
        future = IMRUSerialize.threadPool.submit(new Runnable() {
            @Override
            public void run() {
                Iterator<byte[]> input = io.getInput();
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                try {
                    imruSpec.reduce(imruContext, input, out);
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
                if (imruContext.getIterationNumber() >= parameters.compressIntermediateResultsAfterNIterations)
                    frames.data = IMRUSerialize.decompress(frames.data);
                io.add(frames.data);
                if (debug)
                    Rt.p(curPartition + " received map result "
                            + MergedFrames.deserialize(frames.data));
            }
        } catch (HyracksDataException e) {
            fail();
            throw e;
        } catch (Throwable e) {
            fail();
            throw new HyracksDataException(e);
        }
    }

    void waitForAggregation() throws Exception {
        //        String mapResult = map(context.getNodeId(), curPartition);
        //map complete

        //        synchronized (aggrSync) {
        //              imruSpec.reduce(ctx, input, output);
        //            addResult(mapResult);
        //        }

        if (debug)
            Rt.p("*** Partion " + curPartition
                    + " completing mapping (incoming="
                    + getIncomingPartitionsString() + " target="
                    + targetPartition + ")");
        if (curPartition == nPartitions - 1) //last partition finished;
            Rt.np("--------------------------------");
        swappingTarget = -1;
        while (true) {
            int unfinishedPartition = -1;
            synchronized (aggrSync) {
                boolean hasMissingPartitions = false;
                StringBuilder missing = new StringBuilder();
                for (int id : incomingPartitions) {
                    if (unfinishedPartition < 0 && !receivingPartitions.get(id)
                            && !swapFailedPartitions.get(id))
                        unfinishedPartition = id;
                    if (!receivedPartitions.get(id)) {
                        missing.append(id + ",");
                        hasMissingPartitions = true;
                    }
                }
                if (!fixedTree && unfinishedPartition >= 0) {
                    swappingTarget = unfinishedPartition;
                    swapSucceed = false;
                    swapFailed = false;
                } else {
                    if (hasMissingPartitions) {
                        if (debug)
                            Rt.p(curPartition + " still need " + missing);
                        aggrSync.wait();
                        if (debug)
                            Rt.p(curPartition + " wake up");
                    } else {
                        break;
                    }
                }
            }
            if (swappingTarget >= 0) {
                if (debug) {
                    printAggrTree();
                    Rt.p("*** " + curPartition + " attempts to swap with "
                            + swappingTarget + " "
                            + getIncomingPartitionsString() + "->"
                            + targetPartition);
                }
                isParentNodeOfSwapping = true;
                newChildren = null;
                lockIncomingPartitions(true, swappingTarget);
                while (true) {
                    if (debug)
                        Rt.p(curPartition + " swapping " + swappingTarget + " "
                                + swapSucceed + " " + swapFailed);
                    if (swappingTarget >= 0
                            && (receivingPartitions.get(swappingTarget) || receivedPartitions
                                    .get(swappingTarget))) {
                        if (debug)
                            Rt.p(curPartition + " swapping target finished");
                        swapFailed = true;
                    } else {
                        synchronized (aggrSync) {
                            if (!swapSucceed && !swapFailed)
                                aggrSync.wait();
                        }
                    }

                    if (debug)
                        Rt.p(curPartition + " wake up");
                    if (swapFailed) {
                        if (debug)
                            Rt.p("*** " + curPartition
                                    + " attempts to swap with "
                                    + swappingTarget + " failed");
                        swapFailedPartitions.set(swappingTarget);
                        swappingTarget = -1;
                        break;
                    } else if (swapSucceed) {
                        if (targetPartition >= 0) {
                            SwapChildrenRequest swapChildrenRequest = new SwapChildrenRequest();
                            swapChildrenRequest.removePartition = curPartition;
                            swapChildrenRequest.addPartition = swappingTarget;
                            sendObj(targetPartition, swapChildrenRequest);
                        }
                        SwapTargetRequest swap = new SwapTargetRequest();
                        swap.outgoingPartitionOfSender = targetPartition;
                        swap.newTargetPartition = swappingTarget;
                        for (int id : successfullyHoldPartitions) {
                            if (id != swappingTarget) {
                                if (debug)
                                    Rt.p(curPartition + "->" + targetPartition
                                            + " send swap to " + id + " "
                                            + swap);
                                sendObj(id, swap);
                            }
                        }
                        swap.newTargetPartition = curPartition;
                        if (newChildren != null) {
                            for (int id : newChildren) {
                                if (debug)
                                    Rt.p(curPartition + "->" + targetPartition
                                            + " send swap to " + id + " "
                                            + swap);
                                sendObj(id, swap);
                            }
                        }
                        if (debug)
                            Rt.p(curPartition + "->" + targetPartition
                                    + " send swap to " + swappingTarget + " "
                                    + swap);
                        swap.newTargetPartition = swappingTarget;
                        swap.incompeleteIncomingPartitions = successfullyHoldPartitions;
                        sendObj(swappingTarget, swap);
                        targetPartition = swappingTarget;
                        swapChildren(successfullyHoldPartitions, newChildren,
                                -1);
                        swappingTarget = -1;
                        break;
                    }
                }
            }
        }
        io.close();

        //completed aggregation
        while (true) {
            synchronized (aggrSync) {
                if (holding)
                    aggrSync.wait();
                else {
                    sending = true;
                    break;
                }
            }
        }

        while (aggregatedResult == null) {
            synchronized (aggrSync) {
                if (aggregatedResult == null)
                    aggrSync.wait();
            }
        }
        future.get();

        if (targetPartition < 0) {
            ImruIterationInformation imruRuntimeInformation = new ImruIterationInformation();
            Model model = (Model) imruContext.getModel();
            if (model == null)
                Rt.p("Model == null " + imruContext.getNodeId());
            else {
                Vector<byte[]> v = new Vector<byte[]>();
                v.add(aggregatedResult);
                Model updatedModel = imruSpec.update(imruContext, v.iterator(),
                        model, imruRuntimeInformation);
                long start = System.currentTimeMillis();
                imruRuntimeInformation.object = updatedModel;
                imruRuntimeInformation.currentIteration = imruContext
                        .getIterationNumber();
                imruConnection.uploadModel(modelName, imruRuntimeInformation);
                long end = System.currentTimeMillis();
            }
            //                Rt.p(model);
            //            LOG.info("uploaded model to CC " + (end - start) + " milliseconds");
            Rt.p("Partition " + curPartition + " uploading final result "
                    + MergedFrames.deserialize(aggregatedResult));
        } else {
            if (debug) {
                printAggrTree();
                Rt.p("UPLOAD " + curPartition + " -> " + targetPartition + " "
                        + MergedFrames.deserialize(aggregatedResult));
            }
            sendObj(targetPartition, aggregatedResult);
        }
        targetPartition = -2;
    }

    @Override
    public void fail() throws HyracksDataException {

    }

    @Override
    public void close() throws HyracksDataException {
        //        Rt.p(curPartition + " close");
        try {
            waitForAggregation();
            io.close();
        } catch (Exception e) {
            writer.fail();
            throw new HyracksDataException(e);
        } finally {
            //            Rt.p("close "+ curPartition);
            writer.close();
        }
    }
}