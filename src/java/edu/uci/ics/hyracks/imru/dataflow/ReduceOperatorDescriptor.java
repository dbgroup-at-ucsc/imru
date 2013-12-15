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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.imru.api.ASyncIO;
import edu.uci.ics.hyracks.imru.api.IIMRUJob2;
import edu.uci.ics.hyracks.imru.api.IMRUReduceContext;
import edu.uci.ics.hyracks.imru.api.ImruParameters;
import edu.uci.ics.hyracks.imru.data.ChunkFrameHelper;
import edu.uci.ics.hyracks.imru.data.MergedFrames;
import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * Evaluates the reduce function in an iterative map reduce update job.
 * 
 * @author Josh Rosen
 * @author Rui Wang
 */
public class ReduceOperatorDescriptor extends IMRUOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private static final RecordDescriptor dummyRecordDescriptor = new RecordDescriptor(
            new ISerializerDeserializer[1]);

    private final IIMRUJob2<?, ?> imruSpec;
    public boolean isLocal = false;
    public int level = 0;
    ImruParameters parameters;

    /**
     * Create a new ReduceOperatorDescriptor.
     * 
     * @param spec
     *            The job specification
     * @param imruSpec
     *            The IMRU Job specification
     */
    public ReduceOperatorDescriptor(JobSpecification spec,
            IIMRUJob2<?, ?> imruSpec, String name, ImruParameters parameters) {
        super(spec, 1, 1, name, imruSpec);
        this.imruSpec = imruSpec;
        recordDescriptors[0] = dummyRecordDescriptor;
        this.parameters = parameters;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(
            final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition,
            int nPartitions) throws HyracksDataException {
        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            IMRUReduceContext imruContext;
            Hashtable<Integer, LinkedList<ByteBuffer>> hash = new Hashtable<Integer, LinkedList<ByteBuffer>>();
            public String name;
            private ASyncIO<byte[]> io;
            Future future;

            {
                this.name = ReduceOperatorDescriptor.this.getDisplayName()
                        + partition;
            }

            @Override
            public void open() throws HyracksDataException {
                writer.open();
                imruContext = new IMRUReduceContext(ctx, name, isLocal, level,
                        partition);
                io = new ASyncIO<byte[]>(1);
                future = IMRUSerialize.threadPool.submit(new Runnable() {
                    @Override
                    public void run() {
                        Iterator<byte[]> input = io.getInput();
                        ByteArrayOutputStream out = new ByteArrayOutputStream();
                        try {
                            imruSpec.reduce(imruContext, input, out);
                            byte[] objectData = out.toByteArray();
                            IMRUDebugger.sendDebugInfo(imruContext.getNodeId()
                                    + " reduce start " + partition);
                            if (imruContext.getIterationNumber() >= parameters.compressIntermediateResultsAfterNIterations)
                                objectData = IMRUSerialize.compress(objectData);
                            MergedFrames.serializeToFrames(imruContext, writer,
                                    objectData, partition, imruContext
                                            .getNodeId()
                                            + " reduce "
                                            + partition
                                            + " "
                                            + imruContext.getOperatorName());
                            IMRUDebugger.sendDebugInfo(imruContext.getNodeId()
                                    + " reduce finish");
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
                            encapsulatedChunk, hash, imruContext.getNodeId()
                                    + " recv " + partition + " "
                                    + imruContext.getOperatorName());
                    if (frames.data != null) {
                        if (imruContext.getIterationNumber() >= parameters.compressIntermediateResultsAfterNIterations)
                            frames.data = IMRUSerialize.decompress(frames.data);
                        io.add(frames.data);
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
                //                writer.fail();
            }

            @Override
            public void close() throws HyracksDataException {
                io.close();
                try {
                    future.get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                writer.close();
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
