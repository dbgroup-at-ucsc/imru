package edu.uci.ics.hyracks.imru.elastic;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.imru.api.ImruParameters;
import edu.uci.ics.hyracks.imru.api.ImruStream;
import edu.uci.ics.hyracks.imru.data.SerializedFrames;
import edu.uci.ics.hyracks.imru.dataflow.IMRUOperatorDescriptor;
import edu.uci.ics.hyracks.imru.elastic.wrapper.ImruHyracksWriter;
import edu.uci.ics.hyracks.imru.file.HDFSSplit;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUConnection;
import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * @author Rui Wang
 */
public class ImruSendOD<Model extends Serializable, Data extends Serializable>
        extends IMRUOperatorDescriptor<Model, Data> {
    int[] targetPartitions;
    private final ImruStream<Model, Data> imruSpec;
    ImruParameters parameters;
    String modelName;
    IMRUConnection imruConnection;
    public HDFSSplit[] splits;
    public HDFSSplit[][] allocatedSplits;
    DeploymentId deploymentId;

    //    boolean disableSwapping = false;
    //    int maxWaitTimeBeforeSwap = 1000;
    //    boolean debug;

    public ImruSendOD(DeploymentId deploymentId, JobSpecification spec,
            int[] targets, ImruStream<Model, Data> imruSpec, String name,
            ImruParameters parameters, String modelName,
            IMRUConnection imruConnection) {
        super(spec, parameters.dynamicMapping ? 0 : 1, 1, name, imruSpec);
        this.imruSpec = imruSpec;
        this.parameters = parameters;
        this.deploymentId = deploymentId;
        recordDescriptors[0] = new RecordDescriptor(
                new ISerializerDeserializer[1]);
        targetPartitions = targets;
        this.modelName = modelName;
        this.imruConnection = imruConnection;
        //        this.maxWaitTimeBeforeSwap = maxWaitTimeBeforeSwap;
        //        this.disableSwapping = disableSwapping;
        //        this.debug = debug;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(
            final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider,
            final int curPartition, final int nPartitions)
            throws HyracksDataException {
        try {
            final ImruHyracksWriter hyracksWriter = new ImruHyracksWriter(
                    deploymentId, ctx, null);
            final ImruSendOperator<Model, Data> so = new ImruSendOperator<Model, Data>(
                    hyracksWriter, curPartition, nPartitions, targetPartitions,
                    imruSpec, parameters, modelName, imruConnection, splits,
                    allocatedSplits);

            //        if (parameters.dynamicMapping)
            //            return new ImruSendOperatorDynamic<Model, Data>(so);
            return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {

                @Override
                public void initialize() throws HyracksDataException {
                    //called by dynamic mapping
                    hyracksWriter.writer = writer;
                    try {
                        so.startup();
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                }

                @Override
                public void open() throws HyracksDataException {
                    //called by regular mapping
                    try {
                        so.startup();
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                }

                /**
                 * Called by mapper
                 */
                @Override
                public void nextFrame(ByteBuffer encapsulatedChunk)
                        throws HyracksDataException {
                    //called by regular mapping
                    try {
                        SerializedFrames f = SerializedFrames.nextFrame(ctx
                                .getFrameSize(), encapsulatedChunk);
                        if (f.replyPartition == SerializedFrames.DBG_INFO_FRAME)
                            imruSpec.reduceDbgInfoReceive(f.srcPartition,
                                    f.offset, f.totalSize, f.data,
                                    so.dbgInfoRecvQueue);
                        else
                            imruSpec.reduceReceive(f.srcPartition, f.offset,
                                    f.totalSize, f.data, so.recvQueue);
                    } catch (HyracksDataException e) {
                        fail();
                        throw e;
                    } catch (Throwable e) {
                        fail();
                        throw new HyracksDataException(e);
                    }
                }

                boolean closed = false;

                /**
                 * Called by mapper
                 */
                @Override
                public void close() throws HyracksDataException {
                    if (closed)
                        Rt.p("Closed twice");
                    closed = true;
                    try {
                        so.close();
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                    writer.fail();
                }
            };
        } catch (IOException e1) {
            throw new HyracksDataException(e1);
        }
    }
}
