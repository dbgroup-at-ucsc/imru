package edu.uci.ics.hyracks.imru.dataflow.dynamic;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Random;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.data.MergedFrames;
import edu.uci.ics.hyracks.imru.dataflow.SpreadOD;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUConnection;
import edu.uci.ics.hyracks.imru.util.Rt;

public class ImruRecvOD<Model extends Serializable> extends
        AbstractSingleActivityOperatorDescriptor {
    private final static Logger LOGGER = Logger.getLogger(ImruRecvOD.class
            .getName());

    int[] targets;

    public ImruRecvOD(JobSpecification spec, int[] targets) {
        super(spec, 1, 0);
        this.targets = targets;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(
            final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition,
            final int nPartitions) throws HyracksDataException {
        return new AbstractUnaryInputSinkOperatorNodePushable() {
            Hashtable<Integer, LinkedList<ByteBuffer>> queue = new Hashtable<Integer, LinkedList<ByteBuffer>>();
            IMRUContext context;

            @Override
            public void open() throws HyracksDataException {
                context = new IMRUContext(ctx, "send");
            }

            @Override
            public void nextFrame(ByteBuffer buffer)
                    throws HyracksDataException {
                if (buffer==null)
                    return;
                MergedFrames frames = MergedFrames
                        .nextFrame(ctx, buffer, queue);
                ImruSendOperator callback = (ImruSendOperator) context
                        .getUserObject("aggrRecvCallback");
                if (frames.data == null) {
                    callback.progress(frames.sourceParition,
                            frames.targetParition, frames.receivedSize,
                            frames.totalSize, null);
                    return;
                }
                try {
                    Serializable receivedObject = (Serializable) JavaSerializationUtils
                            .deserialize(frames.data, SpreadOD.class
                                    .getClassLoader());
                    callback.progress(frames.sourceParition,
                            frames.targetParition, frames.receivedSize,
                            frames.totalSize, receivedObject);
                    callback.complete(frames.sourceParition,
                            frames.targetParition, frames.replyPartition,
                            receivedObject);
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                } finally {
                }
            }

            @Override
            public void fail() throws HyracksDataException {
            }

            @Override
            public void close() throws HyracksDataException {
            }
        };
    }
}