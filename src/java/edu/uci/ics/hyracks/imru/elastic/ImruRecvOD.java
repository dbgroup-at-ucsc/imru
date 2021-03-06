package edu.uci.ics.hyracks.imru.elastic;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IJobSerializerDeserializer;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.control.nc.application.NCApplicationContext;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.data.SerializedFrames;
import edu.uci.ics.hyracks.imru.dataflow.SpreadOD;
import edu.uci.ics.hyracks.imru.elastic.swap.IdentificationCorrection;
import edu.uci.ics.hyracks.imru.elastic.swap.IdentifyRequest;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUConnection;
import edu.uci.ics.hyracks.imru.util.Rt;

public class ImruRecvOD<Model extends Serializable> extends
        AbstractSingleActivityOperatorDescriptor {
    private final static Logger LOGGER = Logger.getLogger(ImruRecvOD.class
            .getName());

    DeploymentId deploymentId;
    int[] targets;

    public ImruRecvOD(JobSpecification spec, DeploymentId deploymentId,
            int[] targets) {
        super(spec, 1, 0);
        this.targets = targets;
        this.deploymentId = deploymentId;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(
            final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition,
            final int nPartitions) throws HyracksDataException {
        return new AbstractUnaryInputSinkOperatorNodePushable() {
            //            Hashtable<Integer, LinkedList<ByteBuffer>> queue = new Hashtable<Integer, LinkedList<ByteBuffer>>();
            IMRUContext context;
            ImruSendOperator sendOperator;

            @Override
            public void open() throws HyracksDataException {
                context = new IMRUContext(deploymentId, ctx, "send", partition,
                        nPartitions);
            }

            @Override
            public void nextFrame(ByteBuffer buffer)
                    throws HyracksDataException {
                try {
                    if (buffer == null)
                        return;
                    //                MergedFrames frames = MergedFrames
                    //                        .nextFrame(ctx, buffer, queue);
                    while (sendOperator == null) {
                        sendOperator = (ImruSendOperator) context
                                .getUserObject("sendOperator");
                        Thread.sleep(50);
                    }
                    sendOperator.recvFrame(buffer);
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