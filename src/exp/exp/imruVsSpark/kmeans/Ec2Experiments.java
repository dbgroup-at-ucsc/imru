package exp.imruVsSpark.kmeans;

import java.io.File;
import java.io.PrintStream;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.ec2.HyracksEC2Cluster;
import edu.uci.ics.hyracks.ec2.HyracksEC2Node;
import edu.uci.ics.hyracks.ec2.SSH;
import edu.uci.ics.hyracks.imru.example.utils.Client;
import edu.uci.ics.hyracks.imru.example.utils.ImruEC2;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.imruVsSpark.data.DataGenerator;
import exp.imruVsSpark.kmeans.imru.IMRUKMeans;
import exp.imruVsSpark.kmeans.spark.SparkKMeans;
import exp.test0.GnuPlot;

public class Ec2Experiments {
    String rsync = "rsync -e \"ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no\" -vrultzCc";
    HyracksEC2Cluster cluster;
    HyracksEC2Node[] nodes;
    File resultDir;

    public Ec2Experiments(HyracksEC2Cluster cluster) {
        this.cluster = cluster;
        nodes = cluster.getNodes();
        resultDir = new File("result/ec2_" + nodes.length + "nodes");
        resultDir.mkdir();
    }

    void startSpark() throws Exception {
        //rsync -vrultzCc  /home/wangrui/ucscImru/bin/exp/test0/ ubuntu@ec2-54-242-134-180.compute-1.amazonaws.com:/home/ubuntu/test/bin/exp/test0/
        //screen -d -m -S m /home/ubuntu/spark-0.7.0/run spark.deploy.master.Master -i ${IP} -p 7077
        //screen -d -m -S s /home/ubuntu/spark-0.7.0/run spark.deploy.worker.Worker spark://${IP}:7077
        HyracksEC2Node[] nodes = cluster.getNodes();
        Rt.p("Starting spark");
        Rt.p("http://" + nodes[0].publicIp + ":8080/");
        String master = nodes[0].interalIp;
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
                String cmd = "/home/ubuntu/spark-0.7.0/run spark.deploy.master.Master -i " + master + " -p 7077";
                cmd = "/home/ubuntu/spark-0.7.0/bin/start-master.sh";
                cmd = "nohup bash -c \"/home/ubuntu/spark-0.7.0/run spark.deploy.master.Master -i " + master
                        + " -p 7077 > masterSpark.log 2>&1 \" &";
                Rt.np(cmd);
                ssh.execute(cmd);
                ssh.execute("sleep 5s");
            }
            if (!hasWorker) {
                String cmd = "/home/ubuntu/spark-0.7.0/bin/start-slave.sh spark://" + nodes[0].interalIp + ":7077";
                cmd = "nohup bash -c \"/home/ubuntu/spark-0.7.0/run spark.deploy.worker.Worker spark://" + master
                        + ":7077  > slaveSpark.log 2>&1 \" &";
                Rt.np(cmd);
                ssh.execute(cmd);
            }
            if (i == 0)
                ssh.execute("tail -n 100 /home/ubuntu/masterSpark.log");
            ssh.execute("tail -n 100 /home/ubuntu/slaveSpark.log");
            ssh.close();
        }
        Thread.sleep(2000);
    }

    void stopSpark() throws Exception {
        HyracksEC2Node[] nodes = cluster.getNodes();
        Rt.p("http://" + nodes[0].publicIp + ":8080/");
        SSH ssh = cluster.ssh(0);
        String cmd = "/home/ubuntu/spark-0.7.0/bin/stop-all.sh";
        cmd = "ps aux | grep spark";
        Rt.np(cmd);
        for (String line : ssh.execute(cmd, true).split("\n")) {
            if (line.contains("spark.deploy.master.Master")) {
                String[] ss = line.split("[ |\t]+");
                int pid = Integer.parseInt(ss[1]);
                ssh.execute("kill " + pid);
            }
        }
        ssh.close();
    }

    void stopAll() throws Exception {
        for (HyracksEC2Node node : nodes) {
            SSH ssh = cluster.ssh(node.getNodeId());
            for (String line : ssh.execute("ps aux | grep spark", true).split("\n")) {
                if (line.contains("spark.deploy.master.Master") || line.contains("spark.deploy.worker.Worker")) {
                    String[] ss = line.split("[ |\t]+");
                    int pid = Integer.parseInt(ss[1]);
                    ssh.execute("kill " + pid);
                }
            }
            for (String line : ssh.execute("ps aux | grep Xrunjdwp", true).split("\n")) {
                if (line.contains("hyracks")) {
                    String[] ss = line.split("[ |\t]+");
                    int pid = Integer.parseInt(ss[1]);
                    ssh.execute("kill " + pid);
                }
            }
            for (String line : ssh.execute("ps aux | grep java", true).split("\n")) {
                if (line.contains("hyracks")) {
                    String[] ss = line.split("[ |\t]+");
                    int pid = Integer.parseInt(ss[1]);
                    ssh.execute("kill " + pid);
                }
            }
            //            ssh
            //                    .execute("rm /home/ubuntu/masterSpark.log /home/ubuntu/slaveSpark.log");
            //            ssh.execute("rm /tmp/t1/logs/* /tmp/t2/logs/*");
            ssh.execute("sudo chmod ugo+rw /mnt -R");
            ssh.execute("free -m");
            ssh.close();
        }
    }

    void checkHyracks() throws Exception {
        HyracksConnection hcc = new HyracksConnection(nodes[0].publicIp, 3099);
        Rt.p(hcc.getNodeControllerInfos());
        if (hcc.getNodeControllerInfos().size() != nodes.length) {
            cluster.printLogs(-1, 100);
            throw new Error("Only " + hcc.getNodeControllerInfos().size() + " NC");
        }
    }

    void generateData() throws Exception {
        cluster.startHyrackCluster();
        Thread.sleep(2000);
        checkHyracks();

        PrintStream ps = new PrintStream(new File(resultDir, "generateTime.txt"));
        for (int aaa = EC2Benchmark.STARTC; aaa <= EC2Benchmark.ENDC; aaa += EC2Benchmark.STEPC) {
            DataGenerator.DEBUG_DATA_POINTS = aaa * EC2Benchmark.BATCH;
            int dataSize = DataGenerator.DEBUG_DATA_POINTS * nodes.length;

            long start = System.currentTimeMillis();
            Rt.p("generating data " + DataGenerator.DEBUG_DATA_POINTS + " " + nodes.length);
            //            DataGenerator.main(new String[] { "/home/ubuntu/test/data.txt" });
            IMRUKMeans
                    .generateData(nodes[0].publicIp, DataGenerator.DEBUG_DATA_POINTS, nodes.length, new File(
                            "/home/ubuntu/test/exp_data/product_name"), "/mnt/imru" + aaa + ".txt", "/mnt/spark" + aaa
                            + ".txt");
            long dataTime = System.currentTimeMillis() - start;
            Rt.p(aaa + "\t" + dataTime);
            ps.println(aaa + "\t" + dataTime);
        }
        ps.close();
        cluster.printLogs(-1, 100);
        cluster.stopHyrackCluster();
        for (HyracksEC2Node node : nodes) {
            SSH ssh = cluster.ssh(node.getNodeId());
            Rt.p(node.getName());
            ssh.execute("ll -h /mnt");
            ssh.close();
        }
    }

    void uploadExperimentCode() throws Exception {
        HyracksEC2Node node = nodes[0];
        Rt.p(rsync + " /home/wangrui/ucscImru/bin/ ubuntu@" + node.publicIp + ":/home/ubuntu/test/bin/");
        Rt
                .runAndShowCommand(rsync + " /home/wangrui/ucscImru/bin/ ubuntu@" + node.publicIp
                        + ":/home/ubuntu/test/bin/");
        Rt.runAndShowCommand(rsync + " /home/wangrui/ucscImru/lib/ec2runSpark.sh ubuntu@" + node.publicIp
                + ":/home/ubuntu/test/st.sh");
        Rt.runAndShowCommand(rsync + " /home/wangrui/ucscImru/exp_data/ ubuntu@" + node.publicIp
                + ":/home/ubuntu/test/exp_data/");
    }

    void runImru() throws Exception {
        Rt.p("testing IMRU");
        cluster.startHyrackCluster();
        Thread.sleep(5000);
        checkHyracks();
        SSH ssh = cluster.ssh(0);
        ssh.execute("cd test;");
        ssh
                .execute("sh st.sh exp.imruVsSpark.kmeans.EC2Benchmark " + nodes[0].interalIp + " " + nodes.length
                        + " true");
        String result = new String(Rt.read(ssh.get("/home/ubuntu/test/result/kmeansimru_org.data")));
        Rt.p(result);
        Rt.write(new File(resultDir, "imru.txt"), result.getBytes());
        ssh.close();
        cluster.printLogs(-1, 100);
        cluster.stopHyrackCluster();
    }

    void runSpark() throws Exception {
        Rt.p("testing spark");
        startSpark();
        SSH ssh = cluster.ssh(0);
        ssh.execute("cd test;");
        ssh.execute("sh st.sh exp.imruVsSpark.kmeans.EC2Benchmark " + nodes[0].interalIp + " " + nodes.length
                + " false");
        String result = new String(Rt.read(ssh.get("/home/ubuntu/test/result/kmeansspark_org.data")));
        Rt.p(result);
        Rt.write(new File(resultDir, "spark.txt"), result.getBytes());
        stopSpark();
        ssh.close();
    }

    void runExperiments() throws Exception {
        Rt.p("Spark: http://" + nodes[0].publicIp + ":8080/");
        Rt.p("IMRU: " + cluster.getAdminURL());
        //        stopSpark(cluster);
        //        cluster.stopHyrackCluster();
        stopAll();
        //        System.exit(0);

        uploadExperimentCode();
        generateData();
        stopAll();
        runImru();
        //        stopAll();
        //        runSpark();
    }

    public static void main(String[] args) throws Exception {
        int nodeCount = 5;
        File home = new File(System.getProperty("user.home"));
        File credentialsFile = new File(home, "AwsCredentials.properties");
        File privateKey = new File(home, "ruiwang.pem");
        File hyracksEc2Root = new File(home, "ucscImru/dist");
        ImruEC2 ec2 = new ImruEC2(credentialsFile, privateKey);
        HyracksEC2Cluster cluster = ec2.cluster;
        //        startSpark(ec2.cluster);
        //                stopSpark(ec2.cluster);
        ec2.cluster.MAX_COUNT = 21;
        ec2.cluster.setImageId("ami-1f0c6276");
        ec2.cluster.setMachineType("m1.small");
        //        ec2.setup(hyracksEc2Root, 1, "m1.small");
        ec2.cluster.setTotalInstances(nodeCount);
        ec2.cluster.printNodeStatus();
        if (ec2.cluster.getTotalMachines("stopped") > 0)
            ec2.cluster.startInstances();
        if (ec2.cluster.getTotalMachines("pending") > 0) {
            ec2.cluster.waitForInstanceStart();
            ec2.cluster.printNodeStatus();
        }
        Ec2Experiments exp = new Ec2Experiments(ec2.cluster);
        exp.runExperiments();
        System.exit(0);
    }
}
