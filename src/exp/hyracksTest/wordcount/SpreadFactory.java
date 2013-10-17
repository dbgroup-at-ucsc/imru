package hyracksTest.wordcount;

import java.io.File;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Random;
import java.util.regex.Pattern;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.common.controllers.CCConfig;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.imru.dataflow.SpreadConnectorDescriptor;
import edu.uci.ics.hyracks.imru.dataflow.SpreadOD;
import edu.uci.ics.hyracks.imru.jobgen.SpreadGraph;
import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * @author Rui Wang
 */
public class SpreadFactory {
    public static JobSpecification buildSpreadJob(DeploymentId deploymentId,
            String[] nodes, String startNode, int frameSize, String dataFilePath) {
        nodes = new HashSet<String>(Arrays.asList(nodes))
                .toArray(new String[0]);
        JobSpecification job = new JobSpecification();
        job.setFrameSize(frameSize);

        SpreadGraph graph = new SpreadGraph(nodes, startNode);
        //        graph.print();
        SpreadOD last = null;
        for (int i = 0; i < graph.levels.length; i++) {
            SpreadGraph.Level level = graph.levels[i];
            String[] locations = level.getLocationContraint();
            SpreadOD op = new SpreadOD(deploymentId, job, graph.levels, i,
                    dataFilePath, null, 0, null);
            if (i > 0)
                job.connect(new SpreadConnectorDescriptor(job,
                        graph.levels[i - 1], level), last, 0, op, 0);
            PartitionConstraintHelper.addAbsoluteLocationConstraint(job, op,
                    locations);
            last = op;
        }
        job.addRoot(last);
        return job;
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

        int nodeCount = 17;
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
        DeploymentId deploymentId = hcc.deployBinary(null);

        try {
            String[] ss = new String[nodeCount];
            for (int i = 0; i < ss.length; i++)
                ss[i] = "NC" + i;
            String path = "/tmp/cache/spreadTest${NODE_ID}.dat";
            byte[] bs = new byte[1000];
            Random random = new Random();
            random.nextBytes(bs);
            Rt.write(new File(path.replaceAll(Pattern.quote("${NODE_ID}"),
                    "NC0")), bs);
            JobSpecification job = buildSpreadJob(deploymentId, ss, "NC0", 256,
                    path);

            JobId jobId = hcc.startJob(job, EnumSet.noneOf(JobFlag.class));
            hcc.waitForCompletion(jobId);

            for (int i = 0; i < ss.length; i++) {
                byte[] bs2 = Rt.readFileByte(new File(path.replaceAll(Pattern
                        .quote("${NODE_ID}"), "NC0")));
                if (!Rt.bytesEquals(bs, bs2))
                    throw new Error();
            }
            Rt.p("test passed");
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            Thread.sleep(1000);
            System.exit(0);
        }
    }
}
