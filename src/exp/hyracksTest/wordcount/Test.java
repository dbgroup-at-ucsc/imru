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
package hyracksTest.wordcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Hashtable;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivity;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.common.controllers.CCConfig;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractStateObject;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.imru.api.TupleReader;
import edu.uci.ics.hyracks.imru.api.TupleWriter;
import edu.uci.ics.hyracks.imru.jobgen.OneToOneTuplePartitionComputerFactory;
import edu.uci.ics.hyracks.imru.jobgen.SpreadGraph;
import edu.uci.ics.hyracks.imru.util.Client;
import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * This is a all-in-one word count example to test out hyracks data flow
 * Input file 1: 0a 1b 1c
 * Input file 2: 0b 1c 1c
 * There are two output files. All words starts 0 should be
 * written to output file 0. All words starts 1 should be
 * written to output file 1.
 * For each output file, list words and frequences.
 * Correct output:
 * File 1:
 * 1 0b
 * 1 0a
 * File 2:
 * 3 1c
 * 1 1b
 * This program print out hyracks internal process flow.
 * 
 * @author Rui Wang
 *         Reference: hyracks/hyracks-examples/text-example/textclient/src/main/java/edu/uci/ics/hyracks/examples/text/client
 */
public class Test {
    static class HashGroupState extends AbstractStateObject {
        Hashtable<String, Integer> hash = new Hashtable<String, Integer>();

        public HashGroupState() {
        }

        HashGroupState(JobId jobId, Object id) {
            super(jobId, id);
        }

        @Override
        public void toBytes(DataOutput out) throws IOException {
        }

        @Override
        public void fromBytes(DataInput in) throws IOException {
        }
    }

    public static FileSplit[] parseFileSplits(String fileSplits) {
        String[] splits = fileSplits.split(",");
        FileSplit[] fSplits = new FileSplit[splits.length];
        for (int i = 0; i < splits.length; ++i) {
            String s = splits[i].trim();
            int t = s.indexOf(':');
            fSplits[i] = new FileSplit(s.substring(0, t), new FileReference(
                    new File(s.substring(t + 1))));
        }
        return fSplits;
    }

    public static void createPartitionConstraint(JobSpecification spec,
            IOperatorDescriptor op, FileSplit[] splits) {
        String[] parts = new String[splits.length];
        for (int i = 0; i < splits.length; i++)
            parts[i] = splits[i].getNodeName();
        PartitionConstraintHelper
                .addAbsoluteLocationConstraint(spec, op, parts);
    }

    public static void main(String[] args) throws Exception {
        //start cluster controller
        CCConfig ccConfig = new CCConfig();
        ccConfig.clientNetIpAddress = "127.0.0.1";
        ccConfig.clusterNetIpAddress = "127.0.0.1";
        ccConfig.clusterNetPort = 1099;
        ccConfig.clientNetPort = 3099;
        ccConfig.defaultMaxJobAttempts = 0;
        ccConfig.jobHistorySize = 10;

        //start node controller
        ClusterControllerService cc = new ClusterControllerService(ccConfig);
        cc.start();

        int nodeCount = 3;
        for (int i = 0; i < nodeCount; i++) {
            NCConfig config = new NCConfig();
            config.ccHost = "127.0.0.1";
            config.ccPort = 1099;
            config.clusterNetIPAddress = "127.0.0.1";
            config.dataIPAddress = "127.0.0.1";
            config.nodeId = "NC" + i;
            NodeControllerService nc = new NodeControllerService(config);
            nc.start();
        }

        //connect to hyracks
        IHyracksClientConnection hcc = new HyracksConnection("localhost", 3099);

        //update application
        hcc.deployBinary(null);

        Client.disableLogging();

        try {
            String[] ss = new String[nodeCount];
            for (int i = 0; i < ss.length; i++)
                ss[i] = "NC" + i;
            JobSpecification job = new JobSpecification();
            job.setFrameSize(256);

            SpreadGraph graph = new SpreadGraph(ss, "NC0");
            graph.print();
            SpreadOD last = null;
            for (int i = 0; i < graph.levels.length; i++) {
                SpreadGraph.Level level = graph.levels[i];
                String[] locations = level.getLocationContraint();
                SpreadOD op = new SpreadOD(job, graph.levels, i);
                if (i > 0)
                    job.connect(new SpreadConnectorDescriptor(job,
                            graph.levels[i - 1], level), last, 0, op, 0);
                PartitionConstraintHelper.addAbsoluteLocationConstraint(job,
                        op, locations);
                last = op;
            }
            job.addRoot(last);

            JobId jobId = hcc.startJob(job, EnumSet.noneOf(JobFlag.class));
            hcc.waitForCompletion(jobId);
            for (int i = 0; i < nodeCount; i++)
                Rt.p(i + " " + SpreadOD.result[i]);
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            Thread.sleep(1000);
            System.exit(0);
        }
    }
}