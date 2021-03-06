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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Stack;
import java.util.Vector;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorNodePushable;
import edu.uci.ics.hyracks.imru.api.ASyncIO;
import edu.uci.ics.hyracks.imru.api.ASyncInputStream;
import edu.uci.ics.hyracks.imru.api.DataWriter;
import edu.uci.ics.hyracks.imru.api.FrameWriter;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api.ImruOptions;
import edu.uci.ics.hyracks.imru.api.ImruParameters;
import edu.uci.ics.hyracks.imru.api.ImruStream;
import edu.uci.ics.hyracks.imru.api.TupleReader;
import edu.uci.ics.hyracks.imru.api.old.IIMRUJob2;
import edu.uci.ics.hyracks.imru.data.RunFileContext;
import edu.uci.ics.hyracks.imru.elastic.wrapper.ImruHyracksWriter;
import edu.uci.ics.hyracks.imru.elastic.wrapper.ImruState;
import edu.uci.ics.hyracks.imru.elastic.wrapper.ImruWriter;
import edu.uci.ics.hyracks.imru.file.ConfigurationFactory;
import edu.uci.ics.hyracks.imru.file.HDFSSplit;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRURuntimeContext;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.MapTaskState;
import edu.uci.ics.hyracks.imru.util.IterationUtils;
import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * Parses input data from files in HDFS and caches it on the local file system.
 * During IMRU iterations, these cached examples are processed by the Map
 * operator.
 * 
 * @author Josh Rosen
 */
public class DataLoadOperatorDescriptor extends
        IMRUOperatorDescriptor<Serializable, Serializable> {
    private static final Logger LOG = Logger
            .getLogger(MapOperatorDescriptor.class.getName());

    private static final long serialVersionUID = 1L;

    protected final ConfigurationFactory confFactory;
    protected final HDFSSplit[] inputSplits;
    private boolean hdfsLoad = false;
    //	private boolean memCache;
    ImruParameters parameters;
    DeploymentId deploymentId;

    /**
     * Create a new DataLoadOperatorDescriptor.
     * 
     * @param spec
     *            The Hyracks job specification for the dataflow
     * @param imruSpec
     *            The IMRU job specification
     * @param inputSplits
     *            The files to read the input records from
     * @param confFactory
     *            A Hadoop configuration, used for HDFS.
     */
    public DataLoadOperatorDescriptor(DeploymentId deploymentId,
            JobSpecification spec,
            ImruStream<Serializable, Serializable> imruSpec,
            HDFSSplit[] inputSplits, ConfigurationFactory confFactory,
            boolean hdfsLoad, ImruParameters parameters) {
        super(spec, hdfsLoad ? 1 : 0, 0, "parse", imruSpec);
        this.deploymentId = deploymentId;
        this.inputSplits = inputSplits;
        this.confFactory = confFactory;
        this.hdfsLoad = hdfsLoad;
        this.parameters = parameters;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(
            final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition,
            final int nPartitions) throws HyracksDataException {
        return new AbstractOperatorNodePushable() {
            private final IHyracksTaskContext fileCtx;
            private final String name;
            long startTime;
            ImruState state;
            ImruWriter runFileWriter;
            DataWriter dataWriter;
            IMRUContext imruContext;
            boolean initialized = false;
            ImruHyracksWriter imruWriter;

            {
                fileCtx = new RunFileContext(ctx, imruSpec
                        .getCachedDataFrameSize());
                name = DataLoadOperatorDescriptor.this.getDisplayName()
                        + partition;
                imruWriter = new ImruHyracksWriter(deploymentId,ctx, null);
            }

            @Override
            public void initialize() throws HyracksDataException {
                try {
                    // Rt.p("initialize");
                    if (initialized)
                        return;
                    initialized = true;

                    // Load the examples.
                    MapTaskState state2 = (MapTaskState) IterationUtils
                            .getIterationState(ctx, partition);
                    state = state2 == null ? null : state2.state;
                    if (state != null) {
                        LOG.severe("Duplicate loading of input data.");
                        INCApplicationContext appContext = ctx
                                .getJobletContext().getApplicationContext();
                        IMRURuntimeContext context = (IMRURuntimeContext) appContext
                                .getApplicationObject();
                        context.modelAge = 0;
                        // throw new
                        // IllegalStateException("Duplicate loading of input data.");
                    }
                    startTime = System.currentTimeMillis();
                    if (state == null)
                        state = new ImruState();
                    if (!parameters.useMemoryCache) {
                        FileReference file = ctx
                                .createUnmanagedWorkspaceFile("IMRUInput");
                        runFileWriter = imruWriter.createRunFileWriter();
                        state.diskCache = runFileWriter;
                        runFileWriter.open();
                    } else {
                        Vector vector = new Vector();
                        state.memCache = vector;
                        dataWriter = new DataWriter<Serializable>(vector);
                    }

                    imruContext = new IMRUContext(deploymentId, fileCtx, name,
                            partition, nPartitions);
                    imruContext.setSplit(inputSplits[partition]);
                    if (!hdfsLoad) {
                        final HDFSSplit split = inputSplits[partition];
                        try {
                            InputStream in = split.getInputStream();
                            state.parsedDataSize = in.available();
                            if (runFileWriter != null) {
                                imruSpec
                                        .parse(imruContext,
                                                new BufferedInputStream(in,
                                                        1024 * 1024),
                                                new FrameWriter(runFileWriter));
                            } else {
                                imruSpec
                                        .parse(imruContext,
                                                new BufferedInputStream(in,
                                                        1024 * 1024),
                                                dataWriter);
                            }
                            in.close();
                        } catch (IOException e) {
                            fail();
                            Rt.p(imruContext.getNodeId() + " " + split);
                            throw new HyracksDataException(e);
                        }
                        finishDataCache();
                    }
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

            void finishDataCache() throws IOException {
                if (runFileWriter != null) {
                    runFileWriter.close();
                    LOG.info("Cached input data file "
                            + runFileWriter.getPath() + " is "
                            + runFileWriter.getFileSize() + " bytes");
                }
                long end = System.currentTimeMillis();
                LOG.info("Parsed input data in " + (end - startTime)
                        + " milliseconds");
                IterationUtils
                        .setIterationState(ctx, partition, new MapTaskState(
                                state, ctx.getJobletContext().getJobId(), ctx
                                        .getTaskAttemptId().getTaskId()));
            }

            @Override
            public void setOutputFrameWriter(int index, IFrameWriter writer,
                    RecordDescriptor recordDesc) {
                throw new IllegalArgumentException();
            }

            @Override
            public void deinitialize() throws HyracksDataException {
            }

            @Override
            public int getInputArity() {
                return hdfsLoad ? 1 : 0;
            }

            @Override
            public final IFrameWriter getInputFrameWriter(int index) {
                // Rt.p("getInputFrameWriter");
                try {
                    initialize();
                } catch (HyracksDataException e1) {
                    e1.printStackTrace();
                }
                return new IFrameWriter() {
                    private ASyncIO<byte[]> io;
                    private ASyncInputStream stream;
                    Future future;
                    byte[] ln = "\n".getBytes();

                    @Override
                    public void open() throws HyracksDataException {
                        io = new ASyncIO<byte[]>();
                        stream = new ASyncInputStream(io);
                        future = IMRUSerialize.threadPool
                                .submit(new Runnable() {
                                    @Override
                                    public void run() {
                                        try {
                                            if (runFileWriter != null) {
                                                imruSpec.parse(imruContext,
                                                        stream,
                                                        new FrameWriter(
                                                                runFileWriter));
                                            } else {
                                                imruSpec.parse(imruContext,
                                                        stream, dataWriter);
                                            }

                                            stream.close();
                                        } catch (IOException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                });
                    }

                    @Override
                    public void nextFrame(ByteBuffer buffer)
                            throws HyracksDataException {
                        try {
                            TupleReader reader = new TupleReader(buffer, ctx
                                    .getFrameSize(), 1);
                            while (reader.nextTuple()) {
                                // reader.dump();
                                int len = reader.getFieldLength(0);
                                reader.seekToField(0);
                                byte[] bs = new byte[len];
                                reader.readFully(bs);
                                io.add(bs);
                                // io.add(ln);
                                // String word = new String(bs);
                                // Rt.p(word);
                            }
                            reader.close();
                        } catch (IOException ex) {
                            throw new HyracksDataException(ex);
                        }
                    }

                    @Override
                    public void fail() throws HyracksDataException {
                    }

                    @Override
                    public void close() throws HyracksDataException {
                        io.close();
                        try {
                            future.get();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        try {
                            finishDataCache();
                        } catch (IOException e) {
                            throw new HyracksDataException(e);
                        }
                    }
                };
            }

            private void fail() throws HyracksDataException {
            }
        };
    }
}