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

import scala.actors.threadpool.Arrays;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.imru.api.IMRUReduceContext;
import edu.uci.ics.hyracks.imru.api.ImruFrames;
import edu.uci.ics.hyracks.imru.api.ImruParameters;
import edu.uci.ics.hyracks.imru.api.ImruStream;
import edu.uci.ics.hyracks.imru.data.SerializedFrames;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.swap.IdentifyRequest;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.swap.DynamicCommand;
import edu.uci.ics.hyracks.imru.file.HDFSSplit;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUConnection;
import edu.uci.ics.hyracks.imru.util.Rt;

public class ImruSendOperator<Model extends Serializable, Data extends Serializable>
        extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
    final ImruStream<Model, Data> imruSpec;
    ImruParameters parameters;
    IHyracksTaskContext ctx;
    IMRUReduceContext imruContext;
    String modelName;
    IMRUConnection imruConnection;
    Hashtable<Integer, LinkedList<ByteBuffer>> hash = new Hashtable<Integer, LinkedList<ByteBuffer>>();
    public String name;
    int curPartition;
    int nPartitions;

    MapStates map;
    AggrStates aggr;
    DynamicAggregation<Model, Data> dynamicAggregation = new DynamicAggregation<Model, Data>(
            this);
    IncomingMessageProcessor incomingMessageProcessor = new IncomingMessageProcessor(
            this);
    int[] partitionWriter; //Mapping between partition and writer
    BitSet receivedIdentifications = new BitSet();
    int receivedIdentificationCorrections = 0;
    Object receivedIdentificationSync = new Object();

    //for dynamic mapping
    HDFSSplit[] splits;
    HDFSSplit[][] allocatedSplits;
    AtomicInteger runningMappers;

    public ImruSendOperator(IHyracksTaskContext ctx, int curPartition,
            int nPartitions, int[] targetPartitions,
            ImruStream<Model, Data> imruSpec, ImruParameters parameters,
            String modelName, IMRUConnection imruConnection,
            HDFSSplit[] splits, HDFSSplit[][] allocatedSplits)
            throws HyracksDataException {
        this.ctx = ctx;
        this.name = "DR" + curPartition;
        imruContext = new IMRUReduceContext(ctx, name, false, -1, curPartition,
                nPartitions);
        imruContext.setUserObject("sendOperator", this);
        map = new MapStates(this);
        aggr = new AggrStates(this);
        this.curPartition = curPartition;
        this.nPartitions = nPartitions;
        this.imruSpec = (ImruFrames<Model, Data>) imruSpec;
        this.parameters = parameters;
        this.modelName = modelName;
        this.imruConnection = imruConnection;
        this.splits = splits;
        this.allocatedSplits = allocatedSplits;
        runningMappers = new AtomicInteger(parameters.dynamicMappersPerNode);
        aggr.debug = parameters.dynamicDebug;
        aggr.debugSendOperators[curPartition] = this;
        aggr.targetPartition = targetPartitions[curPartition];
        this.aggr.log.append(aggr.targetPartition + ",");
        int sourceCount = 0;
        for (int i : targetPartitions)
            if (i == curPartition)
                sourceCount++;
        aggr.incomingPartitions = new int[sourceCount];
        sourceCount = 0;
        for (int i = 0; i < targetPartitions.length; i++)
            if (targetPartitions[i] == curPartition)
                aggr.incomingPartitions[sourceCount++] = i;
        map.parent = aggr.targetPartition;
        map.children = Arrays.copyOf(aggr.incomingPartitions,
                aggr.incomingPartitions.length);
        if (map.parent < 0) {
            map.connected = aggr.incomingPartitions;
        } else {
            map.connected = Arrays.copyOf(aggr.incomingPartitions,
                    aggr.incomingPartitions.length + 1);
            map.connected[map.connected.length - 1] = map.parent;
        }
        partitionWriter = new int[nPartitions];
        for (int i = 0; i < nPartitions; i++)
            partitionWriter[i] = i;
    }

    int partitionToWriter(int partitionId) {
        return partitionWriter[partitionId];
    }

    void sendObjToWriter(int targetPartition, DynamicCommand cmd)
            throws IOException {
        if (aggr.debug)
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

    void sendObj(int targetPartition, DynamicCommand cmd) throws IOException {
        if (aggr.debug)
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

    public void complete(int srcPartition, int thisPartition,
            int replyPartition, Object object) throws IOException {
        incomingMessageProcessor.recvObject(srcPartition, thisPartition,
                replyPartition, object);
    }

    Object dbgInfoRecvQueue;
    Object recvQueue;
    byte[] aggregatedResult;

    private void startup() throws HyracksDataException {
        if (parameters.dynamicMapping) {
            if (allocatedSplits == null)
                throw new Error();
            int start = this.curPartition * parameters.dynamicMappersPerNode;
            for (int i = 0; i < parameters.dynamicMappersPerNode; i++) {
                final int mapPartitionId = start + i;
                new Thread("dmap" + mapPartitionId) {
                    public void run() {
                        try {
                            new DynamicMapping(ImruSendOperator.this,
                                    mapPartitionId);
                        } catch (HyracksDataException e) {
                            e.printStackTrace();
                        } finally {
                            int left = runningMappers.decrementAndGet();
                            if (left <= 0) {
                                try {
                                    ImruSendOperator.this.close();
                                } catch (HyracksDataException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                }.start();
            }
        }
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
                synchronized (aggr.aggrSync) {
                    aggr.aggrSync.notifyAll();
                }
                //                IMRUDebugger.sendDebugInfo(imruContext.getNodeId()
                //                        + " reduce finish");
            }
        });
        dbgInfoRecvQueue = imruSpec.reduceDbgInfoInit(imruContext, recvQueue);

        discoverPartitionWriterMapping();
    }

    void discoverPartitionWriterMapping() {
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
                Rt.p(this.curPartition + " still waiting for "
                        + (nPartitions - receivedIdentificationCorrections));
            }
        }
    }

    @Override
    public void initialize() throws HyracksDataException {
        //called by dynamic mapping
        startup();
    }

    @Override
    public void open() throws HyracksDataException {
        //called by regular mapping
        startup();
    }

    /**
     * Called by mapper
     */
    @Override
    public void nextFrame(ByteBuffer encapsulatedChunk)
            throws HyracksDataException {
        //called by regular mapping
        try {
            SerializedFrames f = SerializedFrames.nextFrame(ctx.getFrameSize(),
                    encapsulatedChunk);
            if (f.replyPartition == SerializedFrames.DBG_INFO_FRAME)
                imruSpec.reduceDbgInfoReceive(f.srcPartition, f.offset,
                        f.totalSize, f.data, dbgInfoRecvQueue);
            else
                imruSpec.reduceReceive(f.srcPartition, f.offset, f.totalSize,
                        f.data, recvQueue);
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

    boolean closed = false;

    /**
     * Called by mapper
     */
    @Override
    public void close() throws HyracksDataException {
        if (closed)
            Rt.p("Closed twice");
        closed = true;
        try {
            boolean isRoot = dynamicAggregation.waitForAggregation();
            if (!isRoot)
                Thread.sleep(1000);
        } catch (Exception e) {
            writer.fail();
            throw new HyracksDataException(e);
        } finally {
            writer.close();
        }
    }
}