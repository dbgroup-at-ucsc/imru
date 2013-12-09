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
package exp.imruVsSpark.kmeans.exp.dynamic;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.common.controllers.CCConfig;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNReplicatingConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.imru.api.DataWriter;
import edu.uci.ics.hyracks.imru.api.FrameWriter;
import edu.uci.ics.hyracks.imru.api.IIMRUJob;
import edu.uci.ics.hyracks.imru.api.IIMRUJob2;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api.IMRUDataException;
import edu.uci.ics.hyracks.imru.api.IMRUJob2Impl;
import edu.uci.ics.hyracks.imru.api.IMRUMapContext;
import edu.uci.ics.hyracks.imru.api.IMRUReduceContext;
import edu.uci.ics.hyracks.imru.api.ImruIterationInformation;
import edu.uci.ics.hyracks.imru.api.ImruParameters;
import edu.uci.ics.hyracks.imru.api.ImruSplitInfo;
import edu.uci.ics.hyracks.imru.api.RecoveryAction;
import edu.uci.ics.hyracks.imru.api.TupleWriter;
import edu.uci.ics.hyracks.imru.data.MergedFrames;
import edu.uci.ics.hyracks.imru.dataflow.SpreadConnectorDescriptor;
import edu.uci.ics.hyracks.imru.util.Rt;

public class SRTest {
    public static int[] getAggregationTree(int nodeCount, int arity) {
        if (arity < 2)
            throw new Error();
        int[] targets = new int[nodeCount];
        targets[0] = -1; //root node;
        for (int i = 1; i < targets.length; i++) {
            targets[i] = (i - 1) / arity;
            //            Rt.p(i + "\t" + targets[i]);
        }
        return targets;
    }

    public static JobSpecification createJob(String[] mapOperatorLocations)
            throws InterruptedException, IOException {
        final IIMRUJob<String, String, String> imruSpec = new IIMRUJob<String, String, String>() {
            @Override
            public String update(IMRUContext ctx, Iterator<String> input,
                    String model, ImruIterationInformation iterationInfo)
                    throws IMRUDataException {
                return null;
            }

            @Override
            public boolean shouldTerminate(String model,
                    ImruIterationInformation iterationInfo) {
                return false;
            }

            @Override
            public String reduce(IMRUContext ctx, Iterator<String> input)
                    throws IMRUDataException {
                StringBuilder sb = new StringBuilder();
                while (input.hasNext()) {
                    sb.append(input.next() + ",");
                }
                return sb.toString();
            }

            @Override
            public void parse(IMRUContext ctx, InputStream input,
                    DataWriter<String> output) throws IOException {
            }

            @Override
            public RecoveryAction onJobFailed(
                    List<ImruSplitInfo> completedRanges, long dataSize,
                    int optimalNodesForRerun, float rerunTime,
                    int optimalNodesForPartiallyRerun, float partiallyRerunTime) {
                return null;
            }

            @Override
            public String map(IMRUContext ctx, Iterator<String> input,
                    String model) throws IOException {
                Rt.sleep(Integer.parseInt(ctx.getNodeId().substring(2)) * 1000);
                //                Rt.p("send " + ctx.getNodeId());
                return ctx.getNodeId();
            }

            @Override
            public String integrate(String model1, String model2) {
                return null;
            }

            @Override
            public int getCachedDataFrameSize() {
                return 0;
            }
        };
        final IMRUJob2Impl imru = new IMRUJob2Impl(null, imruSpec);
        int[] targets = getAggregationTree(mapOperatorLocations.length, 2);

        JobSpecification job = new JobSpecification();
        IOperatorDescriptor reader = new AbstractSingleActivityOperatorDescriptor(
                job, 0, 1) {
            {
                recordDescriptors[0] = new RecordDescriptor(
                        new ISerializerDeserializer[1]);
            }

            @Override
            public IOperatorNodePushable createPushRuntime(
                    final IHyracksTaskContext ctx,
                    IRecordDescriptorProvider recordDescProvider,
                    final int partition, int nPartitions) {
                return new AbstractUnaryOutputSourceOperatorNodePushable() {
                    @Override
                    public void initialize() throws HyracksDataException {
                        writer.open();
                        try {
                            ByteBuffer frame = ctx.allocateFrame();
                            ByteArrayOutputStream out = new ByteArrayOutputStream();
                            IMRUMapContext context = new IMRUMapContext(ctx,
                                    "", "");
                            imru.map(context, null, "", out, 1024);
                            byte[] data = out.toByteArray();
                            //                            Rt.p("send " + MergedFrames.deserialize(data)
                            //                                    + " to " + partition);
                            MergedFrames.serializeToFrames(context, frame, ctx
                                    .getFrameSize(), writer, data, partition,
                                    partition, partition, "");
                        } catch (Exception e) {
                            writer.fail();
                            throw new HyracksDataException(e);
                        } finally {
                            writer.close();
                        }
                    }
                };
            }
        };
        ImruParameters parameters = new ImruParameters();
        PartitionConstraintHelper.addAbsoluteLocationConstraint(job, reader,
                mapOperatorLocations);
        ImruSendOD send = new ImruSendOD(job, targets, imru, "abc", parameters,
                null, null);
        //        job.connect(new MToNReplicatingConnectorDescriptor(job), reader, 0,
        //                send, 0);
        job.connect(new OneToOneConnectorDescriptor(job), reader, 0, send, 0);
        //        job.connect(new SpreadConnectorDescriptor(job, null, null), reader, 0,
        //                send, 0);
        ImruRecvOD recv = new ImruRecvOD(job, targets);
        job.connect(new SpreadConnectorDescriptor(job, null, null), send, 0,
                recv, 0);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(job, send,
                mapOperatorLocations);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(job, recv,
                mapOperatorLocations);
        job.addRoot(recv);
        return job;
    }

    public static void main(String[] args) throws Exception {
        Rt.showTime = false;
        //        SendOperator.fixedTree=true;

        //start cluster controller
        CCConfig ccConfig = new CCConfig();
        ccConfig.clientNetIpAddress = "127.0.0.1";
        ccConfig.clusterNetIpAddress = "127.0.0.1";
        ccConfig.clusterNetPort = 1099;
        ccConfig.clientNetPort = 3099;
        ccConfig.defaultMaxJobAttempts = 0;
        ccConfig.jobHistorySize = 10;
        ccConfig.appCCMainClass = "edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUCCBootstrapImpl";

        //start node controller
        ClusterControllerService cc = new ClusterControllerService(ccConfig);
        cc.start();

        String[] nodes = new String[5];
        for (int i = 0; i < nodes.length; i++) {
            NCConfig config = new NCConfig();
            config.ccHost = "127.0.0.1";
            config.ccPort = 1099;
            config.clusterNetIPAddress = "127.0.0.1";
            config.dataIPAddress = "127.0.0.1";
            config.datasetIPAddress = "127.0.0.1";
            config.appNCMainClass = "edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUNCBootstrapImpl";
            nodes[i] = config.nodeId = "NC" + i;
            NodeControllerService nc = new NodeControllerService(config);
            nc.start();
        }

        //connect to hyracks
        IHyracksClientConnection hcc = new HyracksConnection("localhost", 3099);

        //update application
        hcc.deployBinary(null);

        try {

            JobSpecification job = createJob(nodes);

            JobId jobId = hcc.startJob(job, EnumSet.noneOf(JobFlag.class));
            hcc.waitForCompletion(jobId);

        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            Thread.sleep(1000);
            System.exit(0);
        }
    }
}