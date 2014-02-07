package edu.uci.ics.hyracks.imru.elastic.test;

import java.util.EnumSet;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.imru.elastic.AggrStates;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUConnection;
import edu.uci.ics.hyracks.imru.util.CreateDeployment;
import edu.uci.ics.hyracks.imru.util.Rt;

public class DynamicAggregationFunctionalTest {
    public static void functionalTest() throws Exception {
        int nodeCount = 16;
        AggrStates.debug = true;
        AggrStates.debugNodeCount = nodeCount;
        DynamicAggregationStressTest.start(nodeCount);
        HyracksConnection hcc = new HyracksConnection("localhost", 3099);
        //        DeploymentId did = CreateDeployment.uploadApp(hcc);
        DeploymentId did = hcc.deployBinary(null);
        String[] nodes = CreateDeployment.listNodes(hcc);
        try {
            IMRUConnection imruConnection = new IMRUConnection("localhost",
                    3288);
            JobSpecification job = DynamicAggregationStressTest. createJob(did, nodes, "model",
                    imruConnection, false, true);
            JobId jobId = hcc.startJob(did, job, EnumSet.noneOf(JobFlag.class));
            new Thread() {
                public void run() {
                    try {
                        Thread.sleep(500);
                        synchronized ( DynamicAggregationStressTest.sync) {
                            DynamicAggregationStressTest. sync.notifyAll();
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
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            Thread.sleep(1000);
            System.exit(0);
        }
    }
    public static void main(String[] args) throws Exception {
        functionalTest();
    }
}
