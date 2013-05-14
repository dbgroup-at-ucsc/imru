package exp.imruVsSpark.kmeans;

import java.io.File;

import edu.uci.ics.hyracks.ec2.HyracksEC2Cluster;
import edu.uci.ics.hyracks.ec2.HyracksEC2Node;
import edu.uci.ics.hyracks.ec2.Rt;
import edu.uci.ics.hyracks.ec2.SSH;
import edu.uci.ics.hyracks.imru.example.utils.ImruEC2;

public class Ec2Check {
    public static void main(String[] args) throws Exception {
        File home = new File(System.getProperty("user.home"));
        File credentialsFile = new File(home, "AwsCredentials.properties");
        File privateKey = new File(home, "ruiwang.pem");
        ImruEC2 ec2 = new ImruEC2(credentialsFile, privateKey);
        HyracksEC2Cluster cluster = ec2.cluster;
        int lines = 100;
        for (HyracksEC2Node node : cluster.getNodes()) {
            SSH ssh = node.ssh();
            try {
                if (node.getNodeId() == 0) {
                    Rt.np("CC log:");
                    ssh.execute("tail -n " + lines + " /tmp/t1/logs/cc.log");
                }
                Rt.np(node.getName() + " log:");
                ssh.execute("free -m");
                ssh.execute("tail -n " + lines + " /tmp/t2/logs/"
                        + node.getName() + ".log");
            } finally {
                ssh.close();
            }
        }
    }
}
