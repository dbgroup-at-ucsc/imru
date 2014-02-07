/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.imru.dataflow;

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
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.imru.api.ASyncIO;
import edu.uci.ics.hyracks.imru.api.DataWriter;
import edu.uci.ics.hyracks.imru.api.FrameWriter;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api.ImruIterInfo;
import edu.uci.ics.hyracks.imru.api.ImruParameters;
import edu.uci.ics.hyracks.imru.api.ImruStream;
import edu.uci.ics.hyracks.imru.api.old.IIMRUJob2;
import edu.uci.ics.hyracks.imru.data.ChunkFrameHelper;
import edu.uci.ics.hyracks.imru.data.RunFileContext;
import edu.uci.ics.hyracks.imru.data.SerializedFrames;
import edu.uci.ics.hyracks.imru.elastic.wrapper.ImruHyracksWriter;
import edu.uci.ics.hyracks.imru.elastic.wrapper.ImruState;
import edu.uci.ics.hyracks.imru.elastic.wrapper.ImruWriter;
import edu.uci.ics.hyracks.imru.file.HDFSSplit;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRURuntimeContext;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.MapTaskState;
import edu.uci.ics.hyracks.imru.util.IterationUtils;
import edu.uci.ics.hyracks.imru.util.MemoryStatsLogger;
import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * Evaluates the map function in an iterative map reduce update job.
 * 
 * @param <Model>
 *            The class used to represent the global model that is persisted
 *            between iterations.
 * @author Josh Rosen
 * @author Rui Wang
 */
public class MapOperatorDescriptor<Model extends Serializable, Data extends Serializable>
        extends IMRUOperatorDescriptor<Model, Data> {

    private static Logger LOG = Logger.getLogger(MapOperatorDescriptor.class
            .getName());

    private static final long serialVersionUID = 1L;
    private static final RecordDescriptor dummyRecordDescriptor = new RecordDescriptor(
            new ISerializerDeserializer[1]);

    // private final String envInPath;
    protected final HDFSSplit[] inputSplits;
    ImruParameters parameters;
    DeploymentId deploymentId;

    /**
     * Create a new MapOperatorDescriptor.
     * 
     * @param spec
     *            The job specification
     * @param imruSpec
     *            The IMRU job specification
     * @param envInPath
     *            The HDFS path to read the current environment from.
     * @param confFactory
     *            A Hadoop configuration, used for HDFS.
     * @param roundNum
     *            The round number.
     */
    public MapOperatorDescriptor(DeploymentId deploymentId,
            JobSpecification spec, ImruStream<Model, Data> imruSpec,
            HDFSSplit[] inputSplits, String name, ImruParameters parameters) {
        super(spec, 0, 1, name, imruSpec);
        recordDescriptors[0] = dummyRecordDescriptor;
        this.inputSplits = inputSplits;
        this.parameters = parameters;
        this.deploymentId = deploymentId;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(
            final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition,
            final int nPartitions) throws HyracksDataException {
        return new AbstractUnaryOutputSourceOperatorNodePushable() {
            private final IHyracksTaskContext fileCtx;
            private final String name;
            IMRUContext imruContext;
            ImruHyracksWriter imruWriter;
            {
                this.name = MapOperatorDescriptor.this.getDisplayName()
                        + partition;
                fileCtx = new RunFileContext(ctx, imruSpec
                        .getCachedDataFrameSize());
                imruContext = new IMRUContext(deploymentId, ctx, name,
                        partition, nPartitions);
                imruContext.setSplit(inputSplits[partition]);
            }

            @SuppressWarnings("unchecked")
            @Override
            public void initialize() throws HyracksDataException {
                MemoryStatsLogger.logHeapStats(LOG,
                        "MapOperator: Before reading examples");
                imruWriter = new ImruHyracksWriter(deploymentId, ctx,
                        this.writer);
                writer.open();

                try {
                    // Load the environment and weight vector.
                    // For efficiency reasons, the Environment and weight vector
                    // are
                    // shared across all MapOperator partitions.
                    INCApplicationContext appContext = ctx.getJobletContext()
                            .getApplicationContext();
                    IMRURuntimeContext context = (IMRURuntimeContext) appContext
                            .getApplicationObject();
                    context.currentRecoveryIteration = parameters.recoverRoundNum;
                    context.rerunNum = parameters.rerunNum;
                    // final IMRUContext imruContext = new IMRUContext(ctx,
                    // name);
                    Model model = (Model) context.model;
                    if (model == null)
                        throw new HyracksDataException("model is not cached");
                    synchronized (context.envLock) {
                        if (context.modelAge < parameters.roundNum)
                            throw new HyracksDataException(
                                    "Model was not spread to "
                                            + new IMRUContext(deploymentId,
                                                    ctx, name, partition,
                                                    nPartitions).getNodeId());
                    }

                    // Load the examples.
                    MapTaskState state2 = (MapTaskState) IterationUtils
                            .getIterationState(ctx, partition);
                    ImruState state = state2 == null ? null : state2.state;
                    if (!parameters.noDiskCache) {
                        if (state == null) {
                            Rt.p("state=null");
                            System.exit(0);
                            throw new IllegalStateException(
                                    "Input data was not cached");
                        } else {
                            // Use the same state in the future iterations
                            IterationUtils.removeIterationState(ctx, partition);
                            IterationUtils.setIterationState(ctx, partition,
                                    new MapTaskState(state, ctx
                                            .getJobletContext().getJobId(), ctx
                                            .getTaskAttemptId().getTaskId()));
                        }
                    }

                    // Compute the aggregates
                    // To improve the filesystem cache hit rate under a LRU
                    // replacement
                    // policy, alternate the read direction on each round.
                    boolean readInReverse = parameters.roundNum % 2 != 0;
                    LOG.info("Can't read in reverse direction");
                    readInReverse = false;
                    LOG.info("Reading cached input data in "
                            + (readInReverse ? "forwards" : "reverse")
                            + " direction");

                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    ImruIterInfo info;
                    long mapStartTime = System.currentTimeMillis();
                    if (!parameters.noDiskCache) {
                        ImruWriter runFileWriter = state.diskCache;
                        if (runFileWriter != null) {
                            // Read from disk cache
                            Log.info("Cached example file size is "
                                    + runFileWriter.getFileSize() + " bytes");
                            Iterator<ByteBuffer> input = runFileWriter
                                    .getReader().getIterator(
                                            imruSpec.getCachedDataFrameSize());
                            // writer = chunkFrameHelper.wrapWriter(writer,
                            // partition);
                            info = imruSpec.map(imruContext, input, model, out,
                                    imruSpec.getCachedDataFrameSize());
                        } else {
                            // read from memory cache
                            Vector vector = state.memCache;
                            Log.info("Cached in memory examples "
                                    + vector.size());
                            info = imruSpec.mapMem(imruContext,
                                    ((Vector<Data>) vector).iterator(), model,
                                    out, imruSpec.getCachedDataFrameSize());
                            info.op.mappedRecords = vector.size();
                            info.op.totalMappedRecords = vector.size();
                            info.op.mappedDataSize = state.parsedDataSize;
                            info.op.totalMappedDataSize = state.parsedDataSize;
                        }
                    } else {
                        // parse raw data
                        final HDFSSplit split = inputSplits[partition];
                        Log.info("Parse examples " + split.getPath());
                        final ASyncIO<Data> io = new ASyncIO<Data>();
                        final DataWriter<Data> dataWriter = new DataWriter<Data>() {
                            @Override
                            public void addData(Data data) throws IOException {
                                io.add(data);
                            }
                        };
                        ChunkFrameHelper chunkFrameHelper = new ChunkFrameHelper(
                                ctx);
                        final IMRUContext parseContext = new IMRUContext(
                                deploymentId, chunkFrameHelper.getContext(),
                                name, partition, nPartitions);
                        parseContext.setSplit(inputSplits[partition]);

                        Future future = IMRUSerialize.threadPool
                                .submit(new Runnable() {
                                    @Override
                                    public void run() {
                                        try {
                                            InputStream in = split
                                                    .getInputStream();
                                            imruSpec.parse(parseContext,
                                                    new BufferedInputStream(in,
                                                            1024 * 1024),
                                                    dataWriter);

                                            in.close();
                                            io.close();
                                        } catch (IOException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                });

                        info = imruSpec.mapMem(imruContext, io.getInput(),
                                model, out, imruSpec.getCachedDataFrameSize());
                    }
                    info.op.operatorStartTime = mapStartTime;
                    info.op.operatorTotalTime = System.currentTimeMillis()
                            - mapStartTime;
                    byte[] objectData = out.toByteArray();
                    IMRUDebugger.sendDebugInfo(imruContext.getNodeId()
                            + " map start " + partition);
                    if (imruContext.getIterationNumber() >= parameters.compressIntermediateResultsAfterNIterations)
                        objectData = IMRUSerialize.compress(objectData);
                    SerializedFrames.serializeToFrames(imruContext, imruWriter,
                            objectData, partition, 0, imruContext.getNodeId()
                                    + " map " + partition + " "
                                    + imruContext.getOperatorName());
                    SerializedFrames.serializeDbgInfo(imruContext, imruWriter,
                            info, partition, 0, 0);
                    IMRUDebugger.sendDebugInfo(imruContext.getNodeId()
                            + " map finish");
                    writer.close();
                } catch (HyracksDataException e) {
                    writer.fail();
                    throw e;
                } catch (Throwable e) {
                    writer.fail();
                    throw new HyracksDataException(e);
                }
            }
        };
    }
}
