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
import java.io.IOException;
import java.io.OutputStream;
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
import edu.uci.ics.hyracks.imru.api.IMRUReduceContext;
import edu.uci.ics.hyracks.imru.api.ImruIterInfo;
import edu.uci.ics.hyracks.imru.api.ImruParameters;
import edu.uci.ics.hyracks.imru.api.ImruStream;
import edu.uci.ics.hyracks.imru.api.old.IIMRUJob2;
import edu.uci.ics.hyracks.imru.data.ChunkFrameHelper;
import edu.uci.ics.hyracks.imru.data.SerializedFrames;
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

    private final ImruStream<?, ?> imruSpec;
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
            ImruStream<?, ?> imruSpec, String name, ImruParameters parameters) {
        super(spec, 1, 1, name, imruSpec);
        this.imruSpec = imruSpec;
        recordDescriptors[0] = dummyRecordDescriptor;
        this.parameters = parameters;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(
            final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition,
            final int nPartitions) throws HyracksDataException {
        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            IMRUReduceContext imruContext;
            Hashtable<Integer, LinkedList<ByteBuffer>> hash = new Hashtable<Integer, LinkedList<ByteBuffer>>();
            public String name;
            Object dbgInfoRecvQueue;
            Object recvQueue;

            {
                this.name = ReduceOperatorDescriptor.this.getDisplayName()
                        + partition;
            }

            @Override
            public void open() throws HyracksDataException {
                writer.open();
                try {
                    imruContext = new IMRUReduceContext(ctx, name, isLocal,
                            level, partition, nPartitions);
                    IMRUDebugger.sendDebugInfo(imruContext.getNodeId()
                            + " reduce start " + partition);

                    recvQueue = imruSpec.reduceInit(imruContext,
                            new OutputStream() {
                                ByteArrayOutputStream out = new ByteArrayOutputStream();

                                @Override
                                public void write(byte[] b, int off, int len)
                                        throws IOException {
                                    out.write(b, off, len);
                                }

                                @Override
                                public void write(int b) throws IOException {
                                    out.write(b);
                                }

                                @Override
                                public void close() throws IOException {
                                    byte[] objectData = out.toByteArray();
                                    // if (imruContext.getIterationNumber() >=
                                    // parameters.compressIntermediateResultsAfterNIterations)
                                    // objectData = IMRUSerialize.compress(objectData);
                                    SerializedFrames.serializeToFrames(
                                            imruContext, writer, objectData,
                                            partition, 0, imruContext
                                                    .getNodeId()
                                                    + " reduce "
                                                    + partition
                                                    + " "
                                                    + imruContext
                                                            .getOperatorName());
                                    IMRUDebugger.sendDebugInfo(imruContext
                                            .getNodeId()
                                            + " reduce finish");
                                }
                            });
                    dbgInfoRecvQueue = imruSpec.reduceDbgInfoInit(imruContext,
                            recvQueue);
                } catch (Exception e) {
                    e.printStackTrace();
                    try {
                        fail();
                    } catch (HyracksDataException e1) {
                        e1.printStackTrace();
                    }
                }

            }

            @Override
            public void nextFrame(ByteBuffer encapsulatedChunk)
                    throws HyracksDataException {
                try {
                    SerializedFrames f = SerializedFrames.nextFrame(ctx
                            .getFrameSize(), encapsulatedChunk);
                    if (f.replyPartition == SerializedFrames.DBG_INFO_FRAME) {
                        imruSpec.reduceDbgInfoReceive(f.srcPartition, f.offset,
                                f.totalSize, f.data, dbgInfoRecvQueue);
                    } else {
                        imruSpec.reduceReceive(f.srcPartition, f.offset,
                                f.totalSize, f.data, recvQueue);
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
                // writer.fail();
            }

            @Override
            public void close() throws HyracksDataException {
                imruSpec.reduceDbgInfoClose(dbgInfoRecvQueue);
                ImruIterInfo info = imruSpec.reduceClose(recvQueue);
                try {
                    SerializedFrames.serializeDbgInfo(imruContext, writer,
                            info, partition, 0, 0);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
                writer.close();
            }
        };
    }
}
