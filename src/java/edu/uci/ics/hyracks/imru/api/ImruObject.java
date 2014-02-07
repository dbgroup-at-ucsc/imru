package edu.uci.ics.hyracks.imru.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.job.IJobSerializerDeserializer;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.control.nc.application.NCApplicationContext;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import edu.uci.ics.hyracks.imru.api.old.IIMRUJob;
import edu.uci.ics.hyracks.imru.file.HDFSSplit;
import edu.uci.ics.hyracks.imru.util.Rt;

abstract public class ImruObject<Model extends Serializable, Data extends Serializable, IntermediateResult extends Serializable>
        extends ImruFrames<Model, Data> {
    public static final long serialVersionUID = 1;
    int fieldCount = 1;

    @Override
    public void parse(IMRUContext ctx, InputStream in, FrameWriter writer)
            throws IOException {
        TupleWriter tupleWriter = new TupleWriter(
                this.getCachedDataFrameSize(), writer, fieldCount);
        parse(ctx, in, new DataWriter<Data>(tupleWriter));
        tupleWriter.close();
    }

    @Override
    public ImruIterInfo mapMem(IMRUContext ctx, Iterator<Data> input,
            Model model, OutputStream output, int cachedDataFrameSize)
            throws IMRUDataException {
        try {
            IntermediateResult result = map(ctx, input, model);
            byte[] objectData = JavaSerializationUtils.serialize(result);
            output.write(objectData);
            output.close();
            ImruIterInfo r = new ImruIterInfo(ctx);
            HDFSSplit split = ctx.getSplit();
            if (r.op.completedSplits == null)
                r.op.completedSplits = new Vector<HDFSSplit>();
            r.op.completedSplits.add(split);
            r.allCompletedSplits.add(split);
            return r;
        } catch (Exception e) {
            e.printStackTrace();
            throw new IMRUDataException(e);
        }
    }

    public ImruIterInfo map(final IMRUContext ctx, Iterator<ByteBuffer> input,
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
                Data data = (Data) ctx.deserialize(bs);
                r.op.mappedDataSize += bs.length;
                r.op.totalMappedDataSize += bs.length;
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
                    r.op.totalMappedRecords++;
                    return data;
                } catch (Exception e) {
                    throw new ImruError(e);
                }
                //                return null;
            }

            @Override
            public void remove() {
            }
        };
        try {
            IntermediateResult result = map(ctx, dataInterator, model);
            byte[] objectData = JavaSerializationUtils.serialize(result);
            output.write(objectData);
            output.close();
            HDFSSplit split = ctx.getSplit();
            if (r.op.completedSplits == null)
                r.op.completedSplits = new Vector<HDFSSplit>();
            r.op.completedSplits.add(split);
            r.allCompletedSplits.add(split);
            return r;
        } catch (Exception e) {
            e.printStackTrace();
            throw new IMRUDataException(e);
        }
    }

    @Override
    public void reduceFrames(final IMRUContext ctx,
            final Iterator<byte[]> input, OutputStream output)
            throws IMRUDataException {
        Iterator<IntermediateResult> iterator = new Iterator<IntermediateResult>() {
            @Override
            public void remove() {
            }

            @Override
            public boolean hasNext() {
                return input.hasNext();
            }

            @Override
            public IntermediateResult next() {
                byte[] objectData = input.next();
                if (objectData == null)
                    return null;

                try {
                    IntermediateResult result = (IntermediateResult) ctx
                            .deserialize(objectData);
                    return result;
                } catch (Throwable e) {
                    e.printStackTrace();
                }
                return null;
            }
        };
        IntermediateResult result = reduce(ctx, iterator);
        byte[] objectData;
        try {
            objectData = JavaSerializationUtils.serialize(result);
            output.write(objectData);
            output.close();
        } catch (IOException e) {
            throw new IMRUDataException(e);
        }
    }

    @Override
    public Model updateFrames(final IMRUContext ctx,
            final Iterator<byte[]> input, Model model) throws IMRUDataException {
        Iterator<IntermediateResult> iterator = new Iterator<IntermediateResult>() {
            @Override
            public void remove() {
            }

            @Override
            public boolean hasNext() {
                return input.hasNext();
            }

            @Override
            public IntermediateResult next() {
                byte[] objectData = input.next();
                if (objectData == null)
                    return null;
                try {
                    IntermediateResult result = (IntermediateResult) ctx
                            .deserialize(objectData);
                    return result;
                } catch (Exception e) {
                    Rt
                            .p("Read reduce result failed len=%,d",
                                    objectData.length);
                    e.printStackTrace();
                }
                return null;
            }
        };
        return update(ctx, iterator, model);
    }

    /**
     * For a list of data objects, return one result
     */
    abstract public IntermediateResult map(IMRUContext ctx,
            Iterator<Data> input, Model model) throws IOException;

    /**
     * Combine multiple results to one result
     */
    abstract public IntermediateResult reduce(IMRUContext ctx,
            Iterator<IntermediateResult> input) throws IMRUDataException;

    /**
     * update the model using combined result
     */
    abstract public Model update(IMRUContext ctx,
            Iterator<IntermediateResult> input, Model model)
            throws IMRUDataException;

}
