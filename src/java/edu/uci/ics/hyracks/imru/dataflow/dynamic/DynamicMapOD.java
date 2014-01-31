package edu.uci.ics.hyracks.imru.dataflow.dynamic;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
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
import edu.uci.ics.hyracks.imru.api.IMRUMapContext;
import edu.uci.ics.hyracks.imru.api.ImruIterInfo;
import edu.uci.ics.hyracks.imru.api.ImruParameters;
import edu.uci.ics.hyracks.imru.api.ImruStream;
import edu.uci.ics.hyracks.imru.data.ChunkFrameHelper;
import edu.uci.ics.hyracks.imru.data.RunFileContext;
import edu.uci.ics.hyracks.imru.data.SerializedFrames;
import edu.uci.ics.hyracks.imru.dataflow.IMRUDebugger;
import edu.uci.ics.hyracks.imru.dataflow.IMRUOperatorDescriptor;
import edu.uci.ics.hyracks.imru.dataflow.IMRUSerialize;
import edu.uci.ics.hyracks.imru.dataflow.MapOperatorDescriptor;
import edu.uci.ics.hyracks.imru.file.HDFSSplit;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRURuntimeContext;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.MapTaskState;
import edu.uci.ics.hyracks.imru.util.IterationUtils;
import edu.uci.ics.hyracks.imru.util.MemoryStatsLogger;
import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * @author Rui Wang
 * @param <Model>
 * @param <Data>
 */
public class DynamicMapOD<Model extends Serializable, Data extends Serializable>
        extends IMRUOperatorDescriptor<Model, Data> {

    private static Logger LOG = Logger.getLogger(MapOperatorDescriptor.class
            .getName());

    private static final long serialVersionUID = 1L;
    private static final RecordDescriptor dummyRecordDescriptor = new RecordDescriptor(
            new ISerializerDeserializer[1]);

    // private final String envInPath;
    //    private final int roundNum;
    //    int recoverRoundNum;
    //    int rerunNum;
    boolean directParse;
    protected final HDFSSplit[][] allocatedSplits;
    ImruParameters parameters;

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
    public DynamicMapOD(JobSpecification spec,
            ImruStream<Model, Data> imruSpec, HDFSSplit[][] allocatedSplits,
            String name, ImruParameters parameters) {
        super(spec, 0, 1, name, imruSpec);
        recordDescriptors[0] = dummyRecordDescriptor;
        //        this.roundNum = roundNum;
        //        this.recoverRoundNum = recoverRoundNum;
        //        this.rerunNum = rerunNum;
        this.directParse = parameters.noDiskCache;
        this.allocatedSplits = allocatedSplits;
        this.parameters = parameters;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(
            final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition,
            final int nPartitions) throws HyracksDataException {
        return new AbstractUnaryOutputSourceOperatorNodePushable() {
            private final IHyracksTaskContext fileCtx;
            private final String name;
            IMRUMapContext imruContext;

            {
                this.name = DynamicMapOD.this.getDisplayName() + partition;
                fileCtx = new RunFileContext(ctx, imruSpec
                        .getCachedDataFrameSize());
                imruContext = new IMRUMapContext(ctx, name, null, partition,
                        nPartitions);
            }

            MapTaskState loaddata(HDFSSplit split) throws HyracksDataException {
                long startTime = System.currentTimeMillis();
                MapTaskState state = new MapTaskState(ctx.getJobletContext()
                        .getJobId(), ctx.getTaskAttemptId().getTaskId());
                RunFileWriter runFileWriter = null;
                DataWriter dataWriter = null;
                if (!parameters.useMemoryCache) {
                    FileReference file = ctx
                            .createUnmanagedWorkspaceFile("IMRUInput");
                    runFileWriter = new RunFileWriter(file, ctx.getIOManager());
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
                        imruSpec.parse(imruContext, new BufferedInputStream(in,
                                1024 * 1024), new FrameWriter(runFileWriter));
                    } else {
                        imruSpec.parse(imruContext, new BufferedInputStream(in,
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
                LOG.info("Parsed input data in " + (end - startTime)
                        + " milliseconds");
                IterationUtils.setIterationState(ctx, split.uuid, state);
                return state;
            }

            void process(Model model, final HDFSSplit split) throws IOException {
                // Load the examples.
                imruContext.setSplit(split);
                MapTaskState state = (MapTaskState) IterationUtils
                        .getIterationState(ctx, split.uuid);
                if (!directParse) {
                    if (state == null) {
                        if (parameters.dynamicMapping) {
                            state = loaddata(split);
                        } else {
                            Rt.p("state=null");
                            System.exit(0);
                            throw new IllegalStateException(
                                    "Input data was not cached");
                        }
                    } else {
                        // Use the same state in the future iterations
                        IterationUtils.removeIterationState(ctx, split.uuid);
                        IterationUtils.setIterationState(ctx, split.uuid, state);
                    }
                }

                long mapStartTime = System.currentTimeMillis();
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
                if (!directParse) {
                    RunFileWriter runFileWriter = state.getRunFileWriter();
                    if (runFileWriter != null) {
                        info = mapFromDiskCache(runFileWriter, model, out);
                    } else {
                        // read from memory cache
                        Vector vector = state.getMemCache();
                        Log.info("Cached in memory examples " + vector.size());
                        info = imruSpec.mapMem(imruContext,
                                ((Vector<Data>) vector).iterator(), model, out,
                                imruSpec.getCachedDataFrameSize());
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
                    ChunkFrameHelper chunkFrameHelper = new ChunkFrameHelper(
                            ctx);
                    final IMRUMapContext parseContext = new IMRUMapContext(
                            chunkFrameHelper.getContext(), name, split,
                            partition, nPartitions);

                    Future future = IMRUSerialize.threadPool
                            .submit(new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        InputStream in = split.getInputStream();
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

                    info = imruSpec.mapMem(imruContext, io.getInput(), model,
                            out, imruSpec.getCachedDataFrameSize());
                }
                info.op.operatorStartTime = mapStartTime;
                info.op.operatorTotalTime = System.currentTimeMillis()
                        - mapStartTime;
                byte[] objectData = out.toByteArray();
                IMRUDebugger.sendDebugInfo(imruContext.getNodeId()
                        + " map start " + partition);
                if (imruContext.getIterationNumber() >= parameters.compressIntermediateResultsAfterNIterations)
                    objectData = IMRUSerialize.compress(objectData);
                SerializedFrames.serializeToFrames(imruContext, writer,
                        objectData, partition, 0, imruContext.getNodeId()
                                + " map " + partition + " "
                                + imruContext.getOperatorName());
                SerializedFrames.serializeDbgInfo(imruContext, writer, info,
                        partition, 0, 0);
                IMRUDebugger.sendDebugInfo(imruContext.getNodeId()
                        + " map finish");
            }

            @SuppressWarnings("unchecked")
            @Override
            public void initialize() throws HyracksDataException {
                LinkedList<HDFSSplit> queue = imruContext.getQueue();
                synchronized (queue) {
                    for (HDFSSplit split : allocatedSplits[partition]) {
                        if (split.uuid < 0)
                            throw new Error();
                        queue.add(split);
                    }
                }
                INCApplicationContext appContext = ctx.getJobletContext()
                        .getApplicationContext();
                IMRURuntimeContext context = (IMRURuntimeContext) appContext
                        .getApplicationObject();
                MemoryStatsLogger.logHeapStats(LOG,
                        "MapOperator: Before reading examples");
                writer.open();
                try {
                    // Load the environment and weight vector.
                    // For efficiency reasons, the Environment and weight vector
                    // are
                    // shared across all MapOperator partitions.

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
                                            + new IMRUContext(ctx, name,
                                                    partition, nPartitions)
                                                    .getNodeId());
                    }

                    while (true) {
                        HDFSSplit split = null;
                        synchronized (queue) {
                            if (queue.size() == 0)
                                break;
                            split = queue.remove();
                            if (split.uuid < 0)
                                throw new Error();
                        }
                        process(model, split);
                    }
                    writer.close();
                } catch (HyracksDataException e) {
                    writer.fail();
                    throw e;
                } catch (Throwable e) {
                    writer.fail();
                    throw new HyracksDataException(e);
                }
            }

            ImruIterInfo mapFromDiskCache(RunFileWriter runFileWriter,
                    Model model, ByteArrayOutputStream out)
                    throws HyracksDataException {
                // Read from disk cache
                Log.info("Cached example file size is "
                        + runFileWriter.getFileSize() + " bytes");
                final RunFileReader reader = new RunFileReader(runFileWriter
                        .getFileReference(), ctx.getIOManager(), runFileWriter
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
                    return imruSpec.map(imruContext, input, model, out,
                            imruSpec.getCachedDataFrameSize());
                }
            }
        };
    }
}
