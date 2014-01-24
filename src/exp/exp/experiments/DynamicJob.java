package exp.experiments;

import java.util.EnumSet;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.DynamicAggregationStressTest;
import edu.uci.ics.hyracks.imru.example.utils.Client;
import edu.uci.ics.hyracks.imru.util.CreateDeployment;
import edu.uci.ics.hyracks.imru.util.Rt;

public class DynamicJob {
    static void run(String host, boolean disableSwapping) throws Exception {
        HyracksConnection hcc = new HyracksConnection(host, 3099);
        String[] nodes = hcc.getNodeControllerInfos().keySet().toArray(
                new String[0]);
        for (String s : nodes) {
            byte[] bs = hcc.getNodeControllerInfos().get(s).getNetworkAddress()
                    .getIpAddress();
            Rt.p(s + " " + (bs[0] & 0xFF) + "." + (bs[1] & 0xFF) + "."
                    + (bs[2] & 0xFF) + "." + (bs[3] & 0xFF));
        }

        DeploymentId did = CreateDeployment.uploadApp(hcc);
        //update application
        //        hcc.deployBinary(null);

        try {
            JobSpecification job = DynamicAggregationStressTest.createJob(did,
                    nodes, null, null, disableSwapping);
            JobId jobId = hcc.startJob(did, job, EnumSet.noneOf(JobFlag.class));
            hcc.waitForCompletion(jobId);
            Rt.p("finished");
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            Thread.sleep(1000);
            System.exit(0);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0)
            args = new String[] { "192.168.56.102" };
        //        args = new String[] { "localhost" };
        //connect to hyracks
        run(args[0],false);
    }
}
