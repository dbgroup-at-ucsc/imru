package edu.uci.ics.hyracks.imru.elastic;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Vector;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import org.eclipse.jetty.util.log.Log;

import edu.uci.ics.hyracks.imru.api.ASyncIO;
import edu.uci.ics.hyracks.imru.api.DataWriter;
import edu.uci.ics.hyracks.imru.api.FrameWriter;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api.ImruIterInfo;
import edu.uci.ics.hyracks.imru.dataflow.IMRUDebugger;
import edu.uci.ics.hyracks.imru.dataflow.IMRUSerialize;
import edu.uci.ics.hyracks.imru.dataflow.MapOperatorDescriptor;
import edu.uci.ics.hyracks.imru.elastic.map.GetAvailableSplits;
import edu.uci.ics.hyracks.imru.elastic.map.ReplyAvailableSplit;
import edu.uci.ics.hyracks.imru.elastic.map.RequestSplit;
import edu.uci.ics.hyracks.imru.elastic.wrapper.ImruState;
import edu.uci.ics.hyracks.imru.elastic.wrapper.ImruWriter;
import edu.uci.ics.hyracks.imru.file.HDFSSplit;
import edu.uci.ics.hyracks.imru.util.Rt;

public class DynamicMapping<Model extends Serializable, Data extends Serializable> {
    private static Logger LOG = Logger.getLogger(MapOperatorDescriptor.class
            .getName());

    String name;
    ImruSendOperator<Model, Data> so;
    IMRUContext imruContext;
    int partition;
    int nPartitions;
    MapStates m;
    int count = 0;
    ImruIterInfo overallInfo;
    Model model;

    public DynamicMapping(ImruSendOperator<Model, Data> so, int mapPartitionId)
            throws IOException {
        this.so = so;
        this.m = so.map;
        this.partition = mapPartitionId;
        this.nPartitions = so.allocatedSplits.length;
        this.name = "dmap" + partition;
        imruContext = so.platform.getContext(name, mapPartitionId, nPartitions);
        overallInfo = new ImruIterInfo(imruContext);
        imruContext.addSplitsToQueue(so.allocatedSplits[mapPartitionId]);
        try {
            so.runtimeContext.currentRecoveryIteration = so.parameters.recoverRoundNum;
            so.runtimeContext.rerunNum = so.parameters.rerunNum;
            model = (Model) so.runtimeContext.model;
            if (model == null)
                throw new IOException("model is not cached");
            synchronized (so.runtimeContext.envLock) {
                if (so.runtimeContext.modelAge < so.parameters.roundNum)
                    throw new IOException("Model was not spread to "
                            + imruContext.getNodeId());
            }

            long mapStartTime = System.currentTimeMillis();
            while (true) {
                HDFSSplit split = imruContext.popSplitFromQueue();
                if (split == null)
                    break;
                processSplit(model, split);
            }
            if (!so.parameters.disableRelocation)
                requestPartitions();
            overallInfo.op.operatorStartTime = mapStartTime;
            overallInfo.op.operatorTotalTime = System.currentTimeMillis()
                    - mapStartTime;

            byte[] debugInfoData = IMRUSerialize.serialize(overallInfo);
            so.imruSpec.reduceDbgInfoReceive(partition, 0,
                    debugInfoData.length, debugInfoData, so.dbgInfoRecvQueue);
        } catch (IOException e) {
            throw e;
        } catch (Throwable e) {
            throw new IOException(e);
        }
    }

    void requestPartitions() throws IOException, InterruptedException {
        while (m.depletedCount < m.children.length + 1) {
            synchronized (m.queueSync) {
                if (m.incomingQueue.size() == 0) {
                    GetAvailableSplits get = new GetAvailableSplits(
                            so.curPartition);
                    boolean sent = false;
                    for (int p : m.connected) {
                        if (!m.depletedPartitions.get(p)) {
                            if (so.aggr.receivingPartitions.get(p))
                                m.depletedPartitions.set(p);
                            else {
                                so.sendObj(p, get);
                                sent = true;
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

    ImruState loadSplit(HDFSSplit split) throws IOException {
        long startTime = System.currentTimeMillis();
        ImruState state = new ImruState();
        ImruWriter runFileWriter = null;
        DataWriter dataWriter = null;
        if (!so.parameters.useMemoryCache) {
            runFileWriter = state.diskCache = so.platform.createRunFileWriter();
        } else {
            Vector vector = new Vector();
            state.memCache = vector;
            dataWriter = new DataWriter<Serializable>(vector);
        }
        try {
            imruContext.setSplit(split);
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
            throw new IOException(e);
        }
        if (runFileWriter != null) {
            runFileWriter.close();
            LOG.info("Cached input data file " + runFileWriter.getPath()
                    + " is " + runFileWriter.getFileSize() + " bytes");
        }
        long end = System.currentTimeMillis();
        LOG.info("Parsed input data in " + (end - startTime) + " milliseconds");
        so.platform.setIterationState(split.uuid, state);
        return state;
    }

    ImruIterInfo processSplit(Model model, final HDFSSplit split)
            throws IOException {
        // Load the examples.
        imruContext.setSplit(split);
        ImruState state = so.platform.getIterationState(split.uuid);
        if (!so.parameters.noDiskCache) {
            if (state == null) {
                state = loadSplit(split);
            } else {
                // Use the same state in the future iterations
                so.platform.removeIterationState(split.uuid);
                so.platform.setIterationState(split.uuid, state);
            }
        }
        long mapStartTime = System.currentTimeMillis();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ImruIterInfo info;
        if (!so.parameters.noDiskCache) {
            ImruWriter runFileWriter = state.diskCache;
            if (runFileWriter != null) {
                Log.info("Cached example file size is "
                        + runFileWriter.getFileSize() + " bytes");
                try {
                    info = so.imruSpec.map(imruContext, runFileWriter
                            .getReader().getIterator(
                                    so.imruSpec.getCachedDataFrameSize()),
                            model, out, so.imruSpec.getCachedDataFrameSize());
                } catch (Throwable e) {
                    Rt.p(imruContext.getNodeId() + "\t" + split.uuid);
                    throw new IOException(e);
                }
            } else {
                // read from memory cache
                Vector vector = state.memCache;
                LOG.info("Cached in memory examples " + vector.size());
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
            imruContext.setSplit(split);
            Future future = IMRUSerialize.threadPool.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        InputStream in = split.getInputStream();
                        so.imruSpec.parse(imruContext, new BufferedInputStream(
                                in, 1024 * 1024), dataWriter);
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
        info.op.operator = null;
        overallInfo.add(info);
        count++;
        return info;
    }
}
