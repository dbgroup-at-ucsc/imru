package exp.imruVsSpark.kmeans.exp;

import java.util.EnumSet;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.imru.example.utils.Client;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.imruVsSpark.kmeans.exp.dynamic.SRTest;

public class DynamicJob {
    public static void main(String[] args) throws Exception {
        //connect to hyracks
        HyracksConnection hcc = new HyracksConnection(args[0], 3099);
        String[] nodes = hcc.getNodeControllerInfos().keySet().toArray(
                new String[0]);
        for (String s : nodes) {
            Rt.p(s);
        }

        Client.uploadApp(hcc, null, false, 0, "/tmp/cache");
        //update application
        //        hcc.deployBinary(null);

        try {
            JobSpecification job = SRTest.createJob(nodes);
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
