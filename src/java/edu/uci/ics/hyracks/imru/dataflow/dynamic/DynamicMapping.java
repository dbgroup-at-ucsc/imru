package edu.uci.ics.hyracks.imru.dataflow.dynamic;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Vector;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import org.eclipse.jetty.util.log.Log;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.imru.api.ASyncIO;
import edu.uci.ics.hyracks.imru.api.DataWriter;
import edu.uci.ics.hyracks.imru.api.FrameWriter;
import edu.uci.ics.hyracks.imru.api.IMRUMapContext;
import edu.uci.ics.hyracks.imru.api.ImruIterInfo;
import edu.uci.ics.hyracks.imru.data.ChunkFrameHelper;
import edu.uci.ics.hyracks.imru.data.RunFileContext;
import edu.uci.ics.hyracks.imru.dataflow.IMRUDebugger;
import edu.uci.ics.hyracks.imru.dataflow.IMRUSerialize;
import edu.uci.ics.hyracks.imru.dataflow.MapOperatorDescriptor;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.map.GetAvailableSplits;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.map.ReplyAvailableSplit;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.map.RequestSplit;
import edu.uci.ics.hyracks.imru.file.HDFSSplit;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRURuntimeContext;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.MapTaskState;
import edu.uci.ics.hyracks.imru.util.IterationUtils;
import edu.uci.ics.hyracks.imru.util.Rt;

public class DynamicMapping<Model extends Serializable, Data extends Serializable> {
    private static Logger LOG = Logger.getLogger(MapOperatorDescriptor.class
            .getName());

    String name;
    ImruSendOperator<Model, Data> so;
    IMRUMapContext imruContext;
    int partition;
    int nPartitions;
    private final IHyracksTaskContext fileCtx;
    MapStates m;
    int count = 0;
    ImruIterInfo overallInfo;
    Model model;

    public DynamicMapping(ImruSendOperator<Model, Data> so, int mapPartitionId)
            throws HyracksDataException {
        this.so = so;
        this.m = so.map;
        this.partition = mapPartitionId;
        this.nPartitions = so.allocatedSplits.length;
        this.name = "dmap" + partition;
        imruContext = new IMRUMapContext(so.ctx, name, null, mapPartitionId,
                nPartitions);
        overallInfo = new ImruIterInfo(imruContext);
        fileCtx = new RunFileContext(so.ctx, so.imruSpec
                .getCachedDataFrameSize());
        imruContext.addSplitsToQueue(so.allocatedSplits[mapPartitionId]);
        INCApplicationContext appContext = so.ctx.getJobletContext()
                .getApplicationContext();
        IMRURuntimeContext context = (IMRURuntimeContext) appContext
                .getApplicationObject();
        try {
            // Load the environment and weight vector.
            // For efficiency reasons, the Environment and weight vector
            // are
            // shared across all MapOperator partitions.

            context.currentRecoveryIteration = so.parameters.recoverRoundNum;
            context.rerunNum = so.parameters.rerunNum;
            // final IMRUContext imruContext = new IMRUContext(ctx,
            // name);
            model = (Model) context.model;
            if (model == null)
                throw new HyracksDataException("model is not cached");
            synchronized (context.envLock) {
                if (context.modelAge < so.parameters.roundNum)
                    throw new HyracksDataException("Model was not spread to "
                            + imruContext.getNodeId());
            }

            long mapStartTime = System.currentTimeMillis();
            while (true) {
                HDFSSplit split = imruContext.popSplitFromQueue();
                if (split == null)
                    break;
                processSplit(model, split);
            }
            requestPartitions();
            overallInfo.op.operatorStartTime = mapStartTime;
            overallInfo.op.operatorTotalTime = System.currentTimeMillis()
                    - mapStartTime;

            byte[] debugInfoData = JavaSerializationUtils
                    .serialize(overallInfo);
            so.imruSpec.reduceDbgInfoReceive(partition, 0,
                    debugInfoData.length, debugInfoData, so.dbgInfoRecvQueue);
        } catch (HyracksDataException e) {
            throw e;
        } catch (Throwable e) {
            throw new HyracksDataException(e);
        }
    }

    void requestPartitions() throws IOException, InterruptedException {
        while (m.depletedCount < m.children.length + 1) {
            synchronized (m.queueSync) {
                if (m.incomingQueue.size() == 0) {
                    GetAvailableSplits get = new GetAvailableSplits(
                            so.curPartition);
                    boolean sent=false;
                    for (int p : m.connected) {
                        if (!m.depletedPartitions.get(p)) {
                            if (so.aggr.receivingPartitions.get(p))
                                m.depletedPartitions.set(p);
                            else {
                                so.sendObj(p, get);
                                sent=true;
                            }
                        }
                    }
                    if (!sent)
                        return;
                    m.queueSync.wait();
                }
            }
            ReplyAvailableSplit reply = null;
            synchronized (m.queueSync) {
                if (m.incomingQueue.size() > 0)
                    reply = m.incomingQueue.remove();
            }
            if (reply != null) {
                if (reply.splitUUID < 0)
                    throw new Error();
                int uuid = reply.splitUUID;
                RequestSplit request = new RequestSplit(so.curPartition,
                        reply.splitLocation, uuid);
                so.sendObj(reply.splitLocation, request);
                while (true) {
                    synchronized (m.splitSync) {
                        if (m.relocatedSplits.get(uuid)
                                || m.deniedSplits.get(uuid))
                            break;
                        else
                            m.splitSync.wait();
                    }
                }
                if (m.relocatedSplits.get(uuid)) {
                    HDFSSplit split = so.splits[uuid];
                    if (split.uuid != uuid)
                        throw new Error();
                    processSplit(model, split);
                }
            }
        }
    }

    MapTaskState loadSplit(HDFSSplit split) throws HyracksDataException {
        long startTime = System.currentTimeMillis();
        MapTaskState state = new MapTaskState(so.ctx.getJobletContext()
                .getJobId(), so.ctx.getTaskAttemptId().getTaskId());
        RunFileWriter runFileWriter = null;
        DataWriter dataWriter = null;
        if (!so.parameters.useMemoryCache) {
            FileReference file = so.ctx
                    .createUnmanagedWorkspaceFile("IMRUInput");
            runFileWriter = new RunFileWriter(file, so.ctx.getIOManager());
            state.setRunFileWriter(runFileWriter);
            runFileWriter.open();
        } else {
            Vector vector = new Vector();
            state.setMemCache(vector);
            dataWriter = new DataWriter<Serializable>(vector);
        }
        try {
            InputStream in = split.getInputStream();
            state.parsedDataSize = in.available();
            if (runFileWriter != null) {
                so.imruSpec.parse(imruContext, new BufferedInputStream(in,
                        1024 * 1024), new FrameWriter(runFileWriter));
            } else {
                so.imruSpec.parse(imruContext, new BufferedInputStream(in,
                        1024 * 1024), dataWriter);
            }
            in.close();
        } catch (IOException e) {
            Rt.p(imruContext.getNodeId() + " " + split);
            throw new HyracksDataException(e);
        }
        if (runFileWriter != null) {
            runFileWriter.close();
            LOG.info("Cached input data file "
                    + runFileWriter.getFileReference().getFile()
                            .getAbsolutePath() + " is "
                    + runFileWriter.getFileSize() + " bytes");
        }
        long end = System.currentTimeMillis();
        LOG.info("Parsed input data in " + (end - startTime) + " milliseconds");
        IterationUtils.setIterationState(so.ctx, split.uuid, state);
        return state;
    }

    ImruIterInfo processSplit(Model model, final HDFSSplit split)
            throws IOException {
        // Load the examples.
        imruContext.setSplit(split);
        MapTaskState state = (MapTaskState) IterationUtils.getIterationState(
                so.ctx, split.uuid);
        if (!so.parameters.noDiskCache) {
            if (state == null) {
                state = loadSplit(split);
            } else {
                // Use the same state in the future iterations
                IterationUtils.removeIterationState(so.ctx, split.uuid);
                IterationUtils.setIterationState(so.ctx, split.uuid, state);
            }
        }

        long mapStartTime = System.currentTimeMillis();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ImruIterInfo info;
        if (!so.parameters.noDiskCache) {
            RunFileWriter runFileWriter = state.getRunFileWriter();
            if (runFileWriter != null) {
                info = mapFromDiskCache(runFileWriter, model, out);
            } else {
                // read from memory cache
                Vector vector = state.getMemCache();
                Log.info("Cached in memory examples " + vector.size());
                info = so.imruSpec.mapMem(imruContext, ((Vector<Data>) vector)
                        .iterator(), model, out, so.imruSpec
                        .getCachedDataFrameSize());
                info.op.mappedRecords = vector.size();
                info.op.totalMappedRecords = vector.size();
                info.op.mappedDataSize = state.parsedDataSize;
                info.op.totalMappedDataSize = state.parsedDataSize;
            }
        } else {
            // parse raw data
            Log.info("Parse examples " + split.getPath());
            final ASyncIO<Data> io = new ASyncIO<Data>();
            final DataWriter<Data> dataWriter = new DataWriter<Data>() {
                @Override
                public void addData(Data data) throws IOException {
                    io.add(data);
                }
            };
            ChunkFrameHelper chunkFrameHelper = new ChunkFrameHelper(so.ctx);
            final IMRUMapContext parseContext = new IMRUMapContext(
                    chunkFrameHelper.getContext(), name, split, partition,
                    nPartitions);

            Future future = IMRUSerialize.threadPool.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        InputStream in = split.getInputStream();
                        so.imruSpec.parse(parseContext,
                                new BufferedInputStream(in, 1024 * 1024),
                                dataWriter);

                        in.close();
                        io.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });

            info = so.imruSpec.mapMem(imruContext, io.getInput(), model, out,
                    so.imruSpec.getCachedDataFrameSize());
        }
        info.op.operatorStartTime = mapStartTime;
        info.op.operatorTotalTime = System.currentTimeMillis() - mapStartTime;
        byte[] objectData = out.toByteArray();
        IMRUDebugger.sendDebugInfo(imruContext.getNodeId() + " map start "
                + partition);
        if (imruContext.getIterationNumber() >= so.parameters.compressIntermediateResultsAfterNIterations)
            objectData = IMRUSerialize.compress(objectData);
        so.imruSpec.reduceReceive(partition, 0, objectData.length, objectData,
                so.recvQueue);
        //        SerializedFrames.serializeToFrames(imruContext, writer, objectData,
        //                partition, 0, imruContext.getNodeId() + " map " + partition
        //                        + " " + imruContext.getOperatorName());
        //        SerializedFrames.serializeDbgInfo(imruContext, writer, info, partition,
        //                0, 0);
        //        IMRUDebugger.sendDebugInfo(imruContext.getNodeId() + " map finish");
        info.op.operator = null;
        overallInfo.add(info);
        count++;
        return info;
    }

    ImruIterInfo mapFromDiskCache(RunFileWriter runFileWriter, Model model,
            ByteArrayOutputStream out) throws HyracksDataException {
        // Read from disk cache
        Log.info("Cached example file size is " + runFileWriter.getFileSize()
                + " bytes");
        final RunFileReader reader = new RunFileReader(runFileWriter
                .getFileReference(), so.ctx.getIOManager(), runFileWriter
                .getFileSize());
        // readInReverse
        reader.open();
        final ByteBuffer inputFrame = fileCtx.allocateFrame();
        // ChunkFrameHelper chunkFrameHelper = new
        // ChunkFrameHelper(
        // ctx);
        // IMRUContext imruContext = new IMRUContext(
        // chunkFrameHelper.getContext(), name);
        {
            Iterator<ByteBuffer> input = new Iterator<ByteBuffer>() {
                boolean read = false;
                boolean hasData;

                @Override
                public void remove() {
                }

                @Override
                public ByteBuffer next() {
                    if (!hasNext())
                        return null;
                    read = false;
                    return inputFrame;
                }

                @Override
                public boolean hasNext() {
                    try {
                        if (!read) {
                            hasData = reader.nextFrame(inputFrame);
                            read = true;
                        }
                    } catch (HyracksDataException e) {
                        e.printStackTrace();
                    }
                    return hasData;
                }
            };
            // writer = chunkFrameHelper.wrapWriter(writer,
            // partition);
            return so.imruSpec.map(imruContext, input, model, out, so.imruSpec
                    .getCachedDataFrameSize());
        }
    }
}
