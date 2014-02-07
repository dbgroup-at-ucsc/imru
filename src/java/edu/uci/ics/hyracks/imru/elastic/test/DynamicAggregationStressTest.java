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
package edu.uci.ics.hyracks.imru.elastic.test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
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
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api.IMRUDataException;
import edu.uci.ics.hyracks.imru.api.ImruFrames;
import edu.uci.ics.hyracks.imru.api.ImruIterInfo;
import edu.uci.ics.hyracks.imru.api.ImruObject;
import edu.uci.ics.hyracks.imru.api.ImruParameters;
import edu.uci.ics.hyracks.imru.api.ImruSplitInfo;
import edu.uci.ics.hyracks.imru.api.RecoveryAction;
import edu.uci.ics.hyracks.imru.api.old.IIMRUJob;
import edu.uci.ics.hyracks.imru.api.old.IIMRUJob2;
import edu.uci.ics.hyracks.imru.api.old.IMRUJob2Impl;
import edu.uci.ics.hyracks.imru.data.SerializedFrames;
import edu.uci.ics.hyracks.imru.dataflow.SpreadConnectorDescriptor;
import edu.uci.ics.hyracks.imru.elastic.AggrStates;
import edu.uci.ics.hyracks.imru.elastic.ImruRecvOD;
import edu.uci.ics.hyracks.imru.elastic.ImruSendOD;
import edu.uci.ics.hyracks.imru.elastic.wrapper.ImruHyracksWriter;
import edu.uci.ics.hyracks.imru.elastic.wrapper.ImruPlatformAPI;
import edu.uci.ics.hyracks.imru.file.HDFSSplit;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUConnection;
import edu.uci.ics.hyracks.imru.util.CreateDeployment;
import edu.uci.ics.hyracks.imru.util.Rt;

public class DynamicAggregationStressTest {
    public static int interval = 5000;
    public static int modelSize = 10;
    public static Object sync = new Object();

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

    static class Job extends ImruObject<String, String, String> {
        @Override
        public String update(IMRUContext ctx, Iterator<String> input,
                String model) throws IMRUDataException {
            return null;
        }

        @Override
        public boolean shouldTerminate(String model, ImruIterInfo iterationInfo) {
            return false;
        }

        @Override
        public String reduce(IMRUContext ctx, Iterator<String> input)
                throws IMRUDataException {
            StringBuilder sb = new StringBuilder();
            while (input.hasNext()) {
                //                input.next();
                sb.append(input.next() + ",");
            }
            return sb.toString();
            //            char[] cs = new char[modelSize / 2];
            //            return new String(cs);
        }

        @Override
        public void parse(IMRUContext ctx, InputStream input,
                DataWriter<String> output) throws IOException {
        }

        @Override
        public String map(IMRUContext ctx, Iterator<String> input, String model)
                throws IOException {
            //            Rt.sleep(1000);
            try {
                synchronized (sync) {
                    sync.wait();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //            Rt.sleep(Integer.parseInt(ctx.getNodeId().substring(2)) * interval);
            //                Rt.p("send " + ctx.getNodeId());
            return "" + ctx.getPartition();
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

    static class Read extends AbstractSingleActivityOperatorDescriptor {
        ImruObject imru;

        public Read(JobSpecification job, ImruObject imru) {
            super(job, 0, 1);
            this.imru = imru;
            recordDescriptors[0] = new RecordDescriptor(
                    new ISerializerDeserializer[1]);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(
                final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider,
                final int partition, final int nPartitions) {
            return new AbstractUnaryOutputSourceOperatorNodePushable() {
                ImruHyracksWriter w;

                @Override
                public void initialize() throws HyracksDataException {
                    w = new ImruHyracksWriter(null, ctx, writer);
                    writer.open();
                    try {
                        ByteBuffer frame = ctx.allocateFrame();
                        ByteArrayOutputStream out = new ByteArrayOutputStream();
                        IMRUContext context = new IMRUContext(null, ctx, "",
                                partition, nPartitions);
                        imru.map(context, null, "", out, 1024);
                        byte[] data = out.toByteArray();
                        //                            Rt.p("send " + MergedFrames.deserialize(data)
                        //                                    + " to " + partition);
                        SerializedFrames.serializeToFrames(context, frame, ctx
                                .getFrameSize(), w, data, partition, partition,
                                partition, "", partition);
                    } catch (Exception e) {
                        writer.fail();
                        throw new HyracksDataException(e);
                    } finally {
                        writer.close();
                    }
                }
            };
        }
    }

    public static JobSpecification createJob(DeploymentId deploymentId,
            String[] mapOperatorLocations, String modelName,
            IMRUConnection imruConnection, boolean disableSwapping,
            boolean debug) throws InterruptedException, IOException {
        final ImruObject<String, String, String> imruSpec = new Job();
        int[] targets = getAggregationTree(mapOperatorLocations.length, 2);

        JobSpecification job = new JobSpecification();
        IOperatorDescriptor reader = new Read(job, imruSpec);
        ImruParameters parameters = new ImruParameters();
        PartitionConstraintHelper.addAbsoluteLocationConstraint(job, reader,
                mapOperatorLocations);
        parameters.maxWaitTimeBeforeSwap = 0;
        ImruSendOD send = new ImruSendOD(deploymentId, job, targets, imruSpec,
                "abc", parameters, modelName, imruConnection);
        //        job.connect(new MToNReplicatingConnectorDescriptor(job), reader, 0,
        //                send, 0);
        job.connect(new OneToOneConnectorDescriptor(job), reader, 0, send, 0);
        //        job.connect(new SpreadConnectorDescriptor(job, null, null), reader, 0,
        //                send, 0);
        ImruRecvOD recv = new ImruRecvOD(job, deploymentId, targets);
        job.connect(new SpreadConnectorDescriptor(job, null, null), send, 0,
                recv, 0);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(job, send,
                mapOperatorLocations);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(job, recv,
                mapOperatorLocations);
        job.addRoot(recv);
        return job;
    }

    public static void disableLogging() throws Exception {
        Logger globalLogger = Logger.getLogger("");
        Handler[] handlers = globalLogger.getHandlers();
        for (Handler handler : handlers)
            globalLogger.removeHandler(handler);
        globalLogger.addHandler(new Handler() {
            @Override
            public void publish(LogRecord record) {
                String s = record.getMessage();
                if (s.contains("Exception caught by thread")) {
                    System.err.println(s);
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() throws SecurityException {
            }
        });
    }

    public static String[] start(int nodeCount) throws Exception {
        Rt.showTime = false;
        //        SendOperator.fixedTree=true;
        disableLogging();
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

        String[] nodes = new String[nodeCount];
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
        return nodes;
    }

    public static void stressTest() throws Exception {
        int nodeCount = 16;
        //        ImruSendOperator.debug = true;
        AggrStates.debugNodeCount = nodeCount;
        start(nodeCount);
        HyracksConnection hcc = new HyracksConnection("localhost", 3099);
        //        DeploymentId did = CreateDeployment.uploadApp(hcc);
        DeploymentId did = hcc.deployBinary(null);
        String[] nodes = CreateDeployment.listNodes(hcc);
        try {
            IMRUConnection imruConnection = new IMRUConnection("localhost",
                    3288);
            for (int i = 0; i < 10; i++) {
                JobSpecification job = createJob(did, nodes, "model",
                        imruConnection, false, false);
                JobId jobId = hcc.startJob(did, job, EnumSet
                        .noneOf(JobFlag.class));
                new Thread() {
                    public void run() {
                        try {
                            Thread.sleep(500);
                            synchronized (sync) {
                                sync.notifyAll();
                            }
                            while (true) {
                                if (System.in.available() > 0) {
                                    while (System.in.available() > 0)
                                        System.in.read();
                                    AggrStates.printAggrTree();
                                }
                                Thread.sleep(500);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    };
                }.start();
                hcc.waitForCompletion(jobId);
                String s = (String) imruConnection.downloadModel("model");
                Rt.p(s);
                if (!s.startsWith(nodeCount + " "))
                    throw new Error();
            }
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            Thread.sleep(1000);
            System.exit(0);
        }
    }

    public static void main(String[] args) throws Exception {
        stressTest();
        String[] nodes = new String[8];
        for (int i = 0; i < nodes.length; i++)
            nodes[i] = "NC" + i;
        IMRUConnection imruConnection = new IMRUConnection("localhost", 3288);
        JobSpecification job = createJob(null, nodes, "model", imruConnection,
                false, true);
    }
}