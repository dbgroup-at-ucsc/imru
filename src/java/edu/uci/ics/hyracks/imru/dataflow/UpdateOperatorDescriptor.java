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

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.imru.api.ASyncIO;
import edu.uci.ics.hyracks.imru.api.IIMRUJob2;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api.ImruParameters;
import edu.uci.ics.hyracks.imru.api.ImruIterationInformation;
import edu.uci.ics.hyracks.imru.data.ChunkFrameHelper;
import edu.uci.ics.hyracks.imru.data.MergedFrames;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUConnection;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRURuntimeContext;
import edu.uci.ics.hyracks.imru.util.MemoryStatsLogger;
import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * Evaluates the update function in an iterative map reduce update
 * job.
 * <p>
 * The updated model is serialized to a file in HDFS, where it is read by the driver and mappers.
 * 
 * @param <Model>
 *            Josh Rosen
 */
public class UpdateOperatorDescriptor<Model extends Serializable, Data extends Serializable>
        extends IMRUOperatorDescriptor<Model, Data> {

    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(UpdateOperatorDescriptor.class
            .getName());
    private static final RecordDescriptor dummyRecordDescriptor = new RecordDescriptor(
            new ISerializerDeserializer[1]);

    private final String modelName;
    IMRUConnection imruConnection;
    ImruParameters parameters;

    /**
     * Create a new UpdateOperatorDescriptor.
     * 
     * @param spec
     *            The job specification
     * @param imruSpec
     *            The IMRU job specification
     * @param modelInPath
     *            The HDFS path to read the current model from
     * @param confFactory
     *            A Hadoop configuration, used for HDFS.
     * @param envOutPath
     *            The HDFS path to serialize the updated environment
     *            to.
     */
    public UpdateOperatorDescriptor(JobSpecification spec,
            IIMRUJob2<Model, Data> imruSpec, String modelName,
            IMRUConnection imruConnection, ImruParameters parameters) {
        super(spec, 1, 0, "update", imruSpec);
        this.modelName = modelName;
        this.imruConnection = imruConnection;
        this.parameters = parameters;
        //            recordDescriptors[0] = dummyRecordDescriptor;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(
            final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition,
            int nPartitions) throws HyracksDataException {
        return new AbstractUnaryInputSinkOperatorNodePushable() {
            Hashtable<Integer, LinkedList<ByteBuffer>> hash = new Hashtable<Integer, LinkedList<ByteBuffer>>();
            private Model model;
            private final String name;
            IMRUContext imruContext;
            ImruIterationInformation imruRuntimeInformation;

            private ASyncIO<byte[]> io;
            Future future;
            Model updatedModel;

            {
                this.name = UpdateOperatorDescriptor.this.getDisplayName()
                        + partition;
                imruContext = new IMRUContext(ctx, name);
                imruRuntimeInformation = new ImruIterationInformation();
            }

            @SuppressWarnings("unchecked")
            @Override
            public void open() throws HyracksDataException {
                MemoryStatsLogger.logHeapStats(LOG,
                        "Update: Initializing Update");
                model = (Model) imruContext.getModel();
                if (model == null)
                    Rt.p("Model == null " + imruContext.getNodeId());
                io = new ASyncIO<byte[]>();
                future = IMRUSerialize.threadPool.submit(new Runnable() {
                    @Override
                    public void run() {
                        Iterator<byte[]> input = io.getInput();
                        try {
                            updatedModel = imruSpec.update(imruContext, input,
                                    model, imruRuntimeInformation);
                        } catch (HyracksDataException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }

            @Override
            public void nextFrame(ByteBuffer encapsulatedChunk)
                    throws HyracksDataException {
                MergedFrames frames = MergedFrames.nextFrame(ctx,
                        encapsulatedChunk, hash, imruContext.getNodeId()
                                + " recv " + partition + " "
                                + imruContext.getOperatorName());
                if (frames != null) {
                    try {
                        if (imruContext.getIterationNumber() >= parameters.compressIntermediateResultsAfterNIterations)
                            frames.data = IMRUSerialize.decompress(frames.data);
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new HyracksDataException(e);
                    }
                    io.add(frames.data);
                }
            }

            @Override
            public void fail() throws HyracksDataException {
            }

            @Override
            public void close() throws HyracksDataException {
                try {
                    io.close();
                    try {
                        future.get();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    model = (Model) updatedModel;
//                    imruContext.setModel(model);

                    long start = System.currentTimeMillis();
                    imruRuntimeInformation.object = model;
                    imruRuntimeInformation.currentIteration = imruContext
                            .getIterationNumber();
                    imruConnection.uploadModel(modelName,
                            imruRuntimeInformation);
                    long end = System.currentTimeMillis();
                    //                Rt.p(model);
                    LOG.info("uploaded model to CC " + (end - start)
                            + " milliseconds");
                    MemoryStatsLogger.logHeapStats(LOG,
                            "Update: Deinitializing Update");
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

            //            private void enqueueChunk(ByteBuffer chunk, int senderPartition) {
            //                if (bufferedChunks.size() <= senderPartition) {
            //                    for (int i = bufferedChunks.size(); i <= senderPartition; i++) {
            //                        bufferedChunks.add(new LinkedList<ByteBuffer>());
            //                    }
            //                }
            //                bufferedChunks.get(senderPartition).add(chunk);
            //            }
        };
    }

}
