package edu.uci.ics.hyracks.imru.dataflow.dynamic;

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
                context = new IMRUContext(ctx, "send", partition, nPartitions);
            }

            @Override
            public void nextFrame(ByteBuffer buffer)
                    throws HyracksDataException {
                try {
                    if (buffer == null)
                        return;
                    SerializedFrames f = SerializedFrames.nextFrame(ctx
                            .getFrameSize(), buffer);
                    //                MergedFrames frames = MergedFrames
                    //                        .nextFrame(ctx, buffer, queue);
                    while (sendOperator == null) {
                        sendOperator = (ImruSendOperator) context
                                .getUserObject("sendOperator");
                        Thread.sleep(50);
                    }
                    if (f.targetParition != sendOperator.curPartition) {
                        // TODO: find out why this happens
                        if (f.replyPartition == SerializedFrames.DYNAMIC_COMMUNICATION_FRAME) {
                            NCApplicationContext appContext = (NCApplicationContext) ctx
                                    .getJobletContext().getApplicationContext();
                            IJobSerializerDeserializer jobSerDe = appContext
                                    .getJobSerializerDeserializerContainer()
                                    .getJobSerializerDeserializer(deploymentId);
                            Serializable receivedObject = (Serializable) jobSerDe
                                    .deserialize(f.data);
                            if (receivedObject instanceof IdentifyRequest) {
                                IdentificationCorrection c = new IdentificationCorrection(
                                        sendOperator.curPartition,
                                        f.targetParition);
                                for (int i = 0; i < sendOperator.nPartitions; i++) {
                                    sendOperator.sendObj(i, c);
                                }
                            } else if (receivedObject instanceof IdentificationCorrection) {
                            } else {
                                Rt.p("ERROR: " + sendOperator.curPartition
                                        + " recv wrong message from="
                                        + f.srcPartition + " to="
                                        + f.targetParition + " "
                                        + receivedObject);
                                return;
                            }
                            // redeliver

                            //                            sendOperator.getWriter().nextFrame(buffer);
                            //                            throw new Error();
                            //                            System.exit(0);
                        } else {
                            throw new Error(sendOperator.curPartition
                                    + " recv wrong message from="
                                    + f.srcPartition + " to="
                                    + f.targetParition);
                        }
                    }
                    if (f.replyPartition == SerializedFrames.DBG_INFO_FRAME) {
                        boolean completed = sendOperator.imruSpec
                                .reduceDbgInfoReceive(f.srcPartition, f.offset,
                                        f.totalSize, f.data,
                                        sendOperator.dbgInfoRecvQueue);
                        if (completed)
                            sendOperator.completedAggr(f.srcPartition,
                                    f.targetParition, f.replyPartition);
                    } else if (f.replyPartition == SerializedFrames.DYNAMIC_COMMUNICATION_FRAME) {
                        NCApplicationContext appContext = (NCApplicationContext) ctx
                                .getJobletContext().getApplicationContext();
                        IJobSerializerDeserializer jobSerDe = appContext
                                .getJobSerializerDeserializerContainer()
                                .getJobSerializerDeserializer(deploymentId);
                        Serializable receivedObject = (Serializable) jobSerDe
                                .deserialize(f.data);
                        sendOperator.complete(f.srcPartition, f.targetParition,
                                f.replyPartition, receivedObject);
                    } else {
                        sendOperator.imruSpec.reduceReceive(f.srcPartition,
                                f.offset, f.totalSize, f.data,
                                sendOperator.recvQueue);
                        sendOperator.aggrStarted(f.srcPartition,
                                f.targetParition, f.receivedSize, f.totalSize);
                    }
                    //                    if (frames.data == null) {
                    //                        sendOperator.progress(frames.sourceParition,
                    //                                frames.targetParition, frames.receivedSize,
                    //                                frames.totalSize, null);
                    //                        return;
                    //                    }
                    //                    if (ImruSendOperator.debugNetworkSpeed > 0) {
                    //                        Thread
                    //                                .sleep(1 + (int) (frames.data.length / ImruSendOperator.debugNetworkSpeed));
                    //                    }
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