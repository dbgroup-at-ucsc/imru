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
        HyracksEC2Node[] nodes = cluster.getNodes();
        Rt.p("http://" + nodes[0].publicIp + ":8080/");

        HyracksEC2Node node = nodes[0];
        Rt.runAndShowCommand("rsync -vrultzCc /home/wangrui/ucscImru/bin/ ubuntu@" + node.publicIp
                + ":/home/ubuntu/test/bin/");
        //        Rt.runAndShowCommand("rsync -vrultzCc /home/wangrui/ucscImru/lib/ec2runSpark.sh ubuntu@" + node.publicIp
        //                + ":/home/ubuntu/test/st.sh");
        Rt.runAndShowCommand("rsync -vrultzCc /home/wangrui/ucscImru/exp_data/ ubuntu@" + node.publicIp
                + ":/home/ubuntu/test/exp_data/");

        SSH ssh = cluster.ssh(0);
        ssh.execute("cd test;");
        //        ssh.execute("sh st.sh " + nodes[0].interalIp);
        ssh.close();
    }

    public static void exp(String master) throws Exception {
        Client.disableLogging();
        new File("result").mkdir();
        GnuPlot plot = new GnuPlot(new File("result"), "kmeans", "Data points (10^5)", "Time (seconds)");
        plot.extra = "set title \"K=" + DataGenerator.DEBUG_K + ",Iteration=" + DataGenerator.DEBUG_ITERATIONS + "\"";
        plot.setPlotNames("Generate Data", "Bare", "Spark", "IMRU-mem", "IMRU-disk"
//                , "IMRU-parse"
                );
        plot.startPointType = 1;
        plot.pointSize = 1;
        //        plot.reloadData();
        //        for (int i = 0; i < plot.vs.size(); i++)
        //            plot.vs.get(i).set(0, plot.vs.get(i).get(0) / 100000);
        //        plot.finish();
        //        System.exit(0);
        for (DataGenerator.DEBUG_DATA_POINTS = 100000; DataGenerator.DEBUG_DATA_POINTS <= 1000000; DataGenerator.DEBUG_DATA_POINTS += 100000) {
            long start = System.currentTimeMillis();
            DataGenerator.main(new String[] { "/home/ubuntu/test/data.txt" });
            long dataTime = System.currentTimeMillis() - start;

            start = System.currentTimeMillis();
            SparseKMeans.run();
            long bareTime = System.currentTimeMillis() - start;

            start = System.currentTimeMillis();
            IMRUKMeans.runEc2(master, true, false);
            long imruMemTime = System.currentTimeMillis() - start;

//            start = System.currentTimeMillis();
//            IMRUKMeans.runEc2(master, false, true);
//            long imruParseTime = System.currentTimeMillis() - start;

            start = System.currentTimeMillis();
            IMRUKMeans.runEc2(master,false, false);
            long imruDiskTime = System.currentTimeMillis() - start;

            start = System.currentTimeMillis();
            SparkKMeans.run();
            long sparkTime = System.currentTimeMillis() - start;

            Rt.p("Data: %,d", dataTime);
            Rt.p("Bare: %,d", bareTime);
            Rt.p("Spark: %,d", sparkTime);
            Rt.p("IMRU-mem: %,d", imruMemTime);
            Rt.p("IMRU-disk: %,d", imruDiskTime);
//            Rt.p("IMRU-parse: %,d", imruParseTime);

            plot.startNewX(DataGenerator.DEBUG_DATA_POINTS / 100000);
            plot.addY(dataTime / 1000.0);
            plot.addY(bareTime / 1000.0);
            plot.addY(sparkTime / 1000.0);
            plot.addY(imruMemTime / 1000.0);
            plot.addY(imruDiskTime / 1000.0);
//            plot.addY(imruParseTime / 1000.0);
            plot.finish();
        }
        System.exit(0);
    }

    static void imru(HyracksEC2Cluster cluster) throws Exception {
        cluster.startHyrackCluster();
        startSpark(cluster);
    }

    public static void main(String[] args) throws Exception {
        File home = new File(System.getProperty("user.home"));
        File credentialsFile = new File(home, "AwsCredentials.properties");
        File privateKey = new File(home, "ruiwang.pem");
        File hyracksEc2Root = new File(home, "ucscImru/dist");
        ImruEC2 ec2 = new ImruEC2(credentialsFile, privateKey);
        HyracksEC2Cluster cluster = ec2.cluster;
        //        startSpark(ec2.cluster);
        //                stopSpark(ec2.cluster);
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
