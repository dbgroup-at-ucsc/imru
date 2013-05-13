package exp.imruVsSpark;

import java.io.File;

import edu.uci.ics.hyracks.ec2.HyracksEC2Cluster;
import edu.uci.ics.hyracks.ec2.HyracksEC2Node;
import edu.uci.ics.hyracks.ec2.SSH;
import edu.uci.ics.hyracks.imru.example.utils.Client;
import edu.uci.ics.hyracks.imru.example.utils.ImruEC2;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.imruVsSpark.data.DataGenerator;
import exp.imruVsSpark.kmeans.SparseKMeans;
import exp.imruVsSpark.kmeans.imru.IMRUKMeans;
import exp.imruVsSpark.kmeans.spark.SparkKMeans;
import exp.test0.GnuPlot;

public class Ec2Setup {
    static void startSpark(HyracksEC2Cluster cluster) throws Exception {
        //rsync -vrultzCc  /home/wangrui/ucscImru/bin/exp/test0/ ubuntu@ec2-54-242-134-180.compute-1.amazonaws.com:/home/ubuntu/test/bin/exp/test0/
        //screen -d -m -S m /home/ubuntu/spark-0.7.0/run spark.deploy.master.Master -i ${IP} -p 7077
        //screen -d -m -S s /home/ubuntu/spark-0.7.0/run spark.deploy.worker.Worker spark://${IP}:7077
        HyracksEC2Node[] nodes = cluster.getNodes();
        Rt.p("http://" + nodes[0].publicIp + ":8080/");
        for (int i = 0; i < nodes.length; i++) {
            HyracksEC2Node node = nodes[i];
            Rt.p(node.getName());
            SSH ssh = cluster.ssh(i);
            String result = ssh.execute("ls .ssh", true);
            boolean hasSSH = result.contains("id_rsa.pub");
            result = ssh.execute("ps aux | grep spark", true);
            boolean hasMaster = result.contains("spark.deploy.master.Master");
            boolean hasWorker = result.contains("spark.deploy.worker.Worker");

            if (i == 0 && !hasMaster) {
                String cmd = "/home/ubuntu/spark-0.7.0/run spark.deploy.master.Master -i " + nodes[0].interalIp
                        + " -p 7077";
                cmd = "/home/ubuntu/spark-0.7.0/bin/start-master.sh";
                cmd = "nohup /home/ubuntu/spark-0.7.0/run spark.deploy.master.Master -i " + nodes[0].interalIp
                        + " -p 7077 &";
                Rt.np(cmd);
                ssh.execute(cmd);
                ssh.execute("sleep 2s");
            }
            if (!hasWorker) {
                String cmd = "/home/ubuntu/spark-0.7.0/bin/start-slave.sh spark://" + nodes[0].interalIp + ":7077";
                cmd = "nohup /home/ubuntu/spark-0.7.0/run spark.deploy.worker.Worker spark://" + nodes[0].interalIp
                        + ":7077 &";
                Rt.np(cmd);
                ssh.execute(cmd);
            }

            ssh.close();

        }
    }

    static void stopSpark(HyracksEC2Cluster cluster) throws Exception {
        HyracksEC2Node[] nodes = cluster.getNodes();
        Rt.p("http://" + nodes[0].publicIp + ":8080/");
        SSH ssh = cluster.ssh(0);
        String cmd = "/home/ubuntu/spark-0.7.0/bin/stop-all.sh";
        Rt.np(cmd);
        ssh.execute(cmd);
        ssh.close();
    }

    static void testSpark(HyracksEC2Cluster cluster) throws Exception {
        cluster.startHyrackCluster();
        startSpark(cluster);

        HyracksEC2Node[] nodes = cluster.getNodes();
        Rt.p("http://" + nodes[0].publicIp + ":8080/");

        HyracksEC2Node node = nodes[0];
        Rt.runAndShowCommand("rsync -vrultzCc /home/wangrui/ucscImru/bin/ ubuntu@" + node.publicIp
                + ":/home/ubuntu/test/bin/");
        Rt.runAndShowCommand("rsync -vrultzCc /home/wangrui/ucscImru/lib/ec2runSpark.sh ubuntu@" + node.publicIp
                + ":/home/ubuntu/test/st.sh");
        Rt.runAndShowCommand("rsync -vrultzCc /home/wangrui/ucscImru/exp_data/ ubuntu@" + node.publicIp
                + ":/home/ubuntu/test/exp_data/");

        SSH ssh = cluster.ssh(0);
        ssh.execute("cd test;");
        ssh.execute("sh st.sh exp.imruVsSpark.kmeans.EC2Benchmark " + nodes[0].interalIp + " " + nodes.length);
        ssh.close();
    }

    public static void main(String[] args) throws Exception {
        int nodeCount=2;
        File home = new File(System.getProperty("user.home"));
        File credentialsFile = new File(home, "AwsCredentials.properties");
        File privateKey = new File(home, "ruiwang.pem");
        File hyracksEc2Root = new File(home, "ucscImru/dist");
        ImruEC2 ec2 = new ImruEC2(credentialsFile, privateKey);
        HyracksEC2Cluster cluster = ec2.cluster;
        //        startSpark(ec2.cluster);
        //                stopSpark(ec2.cluster);
        ec2.cluster.setImageId("ami-fdff9094");
        ec2.cluster.setMachineType("m1.small");
        //        ec2.setup(hyracksEc2Root, 1, "m1.small");
        ec2.cluster.setTotalInstances(nodeCount);
        testSpark(ec2.cluster);
        
        System.exit(0);
        //        ec2.cluster.setImageId("ami-c9dcb0a0");
        ec2.cluster.setImageId("ami-fdff9094");
        ec2.cluster.setMachineType("m1.small");
        //        ec2.setup(hyracksEc2Root, 1, "m1.small");
        ec2.cluster.setTotalInstances(2);
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
