package edu.uci.ics.hyracks.imru.dataflow.dynamic;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.Random;
import java.util.Vector;

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
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.imru.api.IIMRUJob2;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api.ImruParameters;
import edu.uci.ics.hyracks.imru.data.MergedFrames;
import edu.uci.ics.hyracks.imru.dataflow.IMRUOperatorDescriptor;
import edu.uci.ics.hyracks.imru.file.IMRUFileSplit;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUConnection;
import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * @author Rui Wang
 */
public class ImruSendOD<Model extends Serializable, Data extends Serializable>
        extends IMRUOperatorDescriptor<Model, Data> {
    int[] targetPartitions;
    private final IIMRUJob2<Model, Data> imruSpec;
    ImruParameters parameters;
    String modelName;
    IMRUConnection imruConnection;

    public ImruSendOD(JobSpecification spec, int[] targets,
            IIMRUJob2<Model, Data> imruSpec, String name,
            ImruParameters parameters, String modelName,
            IMRUConnection imruConnection) {
        super(spec, 1, 1, name, imruSpec);
        this.imruSpec = imruSpec;
        this.parameters = parameters;
        recordDescriptors[0] = new RecordDescriptor(
                new ISerializerDeserializer[1]);
        targetPartitions = targets;
        this.modelName = modelName;
        this.imruConnection = imruConnection;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(
            final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider,
            final int curPartition, final int nPartitions)
            throws HyracksDataException {
        return new ImruSendOperator<Model, Data>(ctx, curPartition,
                nPartitions, targetPartitions, imruSpec, parameters, modelName,
                imruConnection);
    }
}
