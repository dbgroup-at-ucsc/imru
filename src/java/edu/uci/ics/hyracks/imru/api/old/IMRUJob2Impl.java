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

package edu.uci.ics.hyracks.imru.api.old;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IJobSerializerDeserializer;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.control.nc.application.NCApplicationContext;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import edu.uci.ics.hyracks.imru.api.DataWriter;
import edu.uci.ics.hyracks.imru.api.FrameWriter;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api.IMRUDataException;
import edu.uci.ics.hyracks.imru.api.ImruIterInfo;
import edu.uci.ics.hyracks.imru.api.ImruSplitInfo;
import edu.uci.ics.hyracks.imru.api.RecoveryAction;
import edu.uci.ics.hyracks.imru.api.TupleReader;
import edu.uci.ics.hyracks.imru.api.TupleWriter;
import edu.uci.ics.hyracks.imru.dataflow.IMRUSerialize;
import edu.uci.ics.hyracks.imru.util.Rt;

public class IMRUJob2Impl<Model extends Serializable, Data extends Serializable, T extends Serializable>
        implements IIMRUJob2<Model, Data> {
    public static final long serialVersionUID = 1;
    int fieldCount = 1;
    DeploymentId deploymentId;
    IIMRUJob<Model, Data, T> job;
    private static ExecutorService threadPool = Executors.newCachedThreadPool();

    public IMRUJob2Impl(DeploymentId deploymentId, IIMRUJob<Model, Data, T> job) {
        this.deploymentId = deploymentId;
        this.job = job;
    }

    @Override
    public int getCachedDataFrameSize() {
        return job.getCachedDataFrameSize();
    }

    @Override
    public void parse(IMRUContext ctx, InputStream input,
            DataWriter<Data> output) throws IOException {
        job.parse(ctx, input, output);
    }

    @Override
    public void parse(IMRUContext ctx, InputStream in, FrameWriter writer)
            throws IOException {
        TupleWriter tupleWriter = new TupleWriter(ctx, writer, fieldCount);
        job.parse(ctx, in, new DataWriter<Data>(tupleWriter));
        tupleWriter.close();
    }

    public void mapMem(IMRUContext ctx, java.util.Iterator<Data> input,
            Model model, OutputStream output, int cachedDataFrameSize)
            throws IMRUDataException {
        try {
            //            ImruIterInfo r = new ImruIterInfo<T>();
            Serializable object = job.map(ctx, input, model);
            //            r.completedPaths.add(((IMRUMapContext) ctx).getDataPath());
            byte[] objectData = JavaSerializationUtils.serialize(object);
            output.write(objectData);
            output.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IMRUDataException(e);
        }
    }

    @Override
    public void map(final IMRUContext ctx, Iterator<ByteBuffer> input,
            Model model, OutputStream output, int cachedDataFrameSize)
            throws IMRUDataException {
        final ImruIterInfo r = new ImruIterInfo(ctx);
        FrameTupleAccessor accessor = new FrameTupleAccessor(
                cachedDataFrameSize, new RecordDescriptor(
                        new ISerializerDeserializer[fieldCount]));
        final TupleReader reader = new TupleReader(input, accessor,
                new ByteBufferInputStream());
        Iterator<Data> dataInterator = new Iterator<Data>() {
            @Override
            public boolean hasNext() {
                return reader.hasNextTuple();
            }

            public Data read() throws Exception {
                int length = reader.readInt();
                if (length == 0)
                    throw new Exception("map read 0");
                byte[] bs = new byte[length];
                int len = reader.read(bs);
                if (len != length)
                    throw new Exception("partial read");
                NCApplicationContext appContext = (NCApplicationContext) ctx
                        .getJobletContext().getApplicationContext();
                IJobSerializerDeserializer jobSerDe = appContext
                        .getJobSerializerDeserializerContainer()
                        .getJobSerializerDeserializer(deploymentId);
                Data data = (Data) jobSerDe.deserialize(bs);
                r.op.mappedDataSize += bs.length;
                return data;
            }

            @Override
            public Data next() {
                if (!hasNext())
                    return null;
                try {
                    reader.nextTuple();
                    Data data = read();
                    r.op.mappedRecords++;
                    return data;
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public void remove() {
            }
        };
        try {
            Serializable object = job.map(ctx, dataInterator, model);
            //            r.completedPaths.add(((IMRUMapContext) ctx).getDataPath());
            byte[] objectData = JavaSerializationUtils.serialize(object);
            output.write(objectData);
            output.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IMRUDataException(e);
        }
    }

    @Override
    public void reduce(final IMRUContext ctx, final Iterator<byte[]> input,
            OutputStream output) throws IMRUDataException {
        final ImruIterInfo r = new ImruIterInfo(ctx);
        Iterator<T> iterator = new Iterator<T>() {
            @Override
            public void remove() {
            }

            @Override
            public boolean hasNext() {
                return input.hasNext();
            }

            @Override
            public T next() {
                byte[] objectData = input.next();
                if (objectData == null)
                    return null;

                NCApplicationContext appContext = (NCApplicationContext) ctx
                        .getJobletContext().getApplicationContext();
                IJobSerializerDeserializer jobSerDe = appContext
                        .getJobSerializerDeserializerContainer()
                        .getJobSerializerDeserializer(deploymentId);
                try {
                    T o = (T) jobSerDe.deserialize(objectData);
                    //                    r.add(r2);
                    return o;
                } catch (Throwable e) {
                    e.printStackTrace();
                }
                return null;
            }
        };
        T object = job.reduce(ctx, iterator);
        byte[] objectData;
        try {
            objectData = JavaSerializationUtils.serialize(r);
            output.write(objectData);
            output.close();
        } catch (IOException e) {
            throw new IMRUDataException(e);
        }
    }

    public boolean shouldTerminate(Model model, ImruIterInfo runtimeInformation) {
        return job.shouldTerminate(model, runtimeInformation);
    }

    @Override
    public Model update(final IMRUContext ctx, final Iterator<byte[]> input,
            Model model, final ImruIterInfo r) throws IMRUDataException {
        Iterator<T> iterator = new Iterator<T>() {
            @Override
            public void remove() {
            }

            @Override
            public boolean hasNext() {
                return input.hasNext();
            }

            @Override
            public T next() {
                byte[] objectData = input.next();
                if (objectData == null)
                    return null;
                NCApplicationContext appContext = (NCApplicationContext) ctx
                        .getJobletContext().getApplicationContext();
                IJobSerializerDeserializer jobSerDe = appContext
                        .getJobSerializerDeserializerContainer()
                        .getJobSerializerDeserializer(deploymentId);
                try {
                    T r2 = (T) jobSerDe.deserialize(objectData);
                    //                    r.add(r2);
                    return r2;
                } catch (Exception e) {
                    Rt
                            .p("Read reduce result failed len=%,d",
                                    objectData.length);
                    e.printStackTrace();
                }
                return null;
            }
        };
        return job.update(ctx, iterator, model, r);
    }

    @Override
    public Model integrate(Model model1, Model model2) {
        return job.integrate(model1, model2);
    }

    @Override
    public RecoveryAction onJobFailed(List<ImruSplitInfo> completedRanges,
            long dataSize, int optimalNodesForRerun, float rerunTime,
            int optimalNodesForPartiallyRerun, float partiallyRerunTime) {
        return job.onJobFailed(completedRanges, dataSize, optimalNodesForRerun,
                rerunTime, optimalNodesForPartiallyRerun, partiallyRerunTime);
    }
}