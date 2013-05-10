package exp.imruVsSpark;

import java.io.File;

import edu.uci.ics.hyracks.ec2.HyracksEC2Cluster;
import edu.uci.ics.hyracks.imru.example.utils.ImruEC2;

public class Ec2Setup {
    public static void main(String[] args) throws Exception {
        File home = new File(System.getProperty("user.home"));
        File credentialsFile = new File(home, "AwsCredentials.properties");
        File privateKey = new File(home, "ruiwang.pem");
        File hyracksEc2Root = new File(home, "ucscImru/dist");
        ImruEC2 ec2 = new ImruEC2(credentialsFile, privateKey);
        ec2.cluster.setImageId("ami-c9dcb0a0");
        //        ec2.setup(hyracksEc2Root, 1, "m1.small");
        ec2.cluster.setMachineType("m1.small");
        ec2.cluster.setTotalInstances(1);
        ec2.cluster.printNodeStatus();
        if (ec2.cluster.getTotalMachines("stopped") > 0)
            ec2.cluster.startInstances();
        if (ec2.cluster.getTotalMachines("pending") > 0) {
            ec2.cluster.waitForInstanceStart();
            ec2.cluster.printNodeStatus();
        }
        //        ec2.setup(hyracksEc2Root, 1, "t1.micro");
        //        ec2.cluster.install(hyracksEc2Root);
        //        ec2.cluster.stopHyrackCluster();
        //        ec2.cluster.startHyrackCluster();
        System.exit(0);
    }
}
