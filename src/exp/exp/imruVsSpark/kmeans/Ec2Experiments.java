package exp.imruVsSpark.kmeans;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.ec2.HyracksCluster;
import edu.uci.ics.hyracks.ec2.HyracksEC2Cluster;
import edu.uci.ics.hyracks.ec2.HyracksEC2Node;
import edu.uci.ics.hyracks.ec2.HyracksNode;
import edu.uci.ics.hyracks.ec2.SSH;
import edu.uci.ics.hyracks.imru.example.utils.Client;
import edu.uci.ics.hyracks.imru.example.utils.ImruEC2;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.ClusterMonitor;
import exp.imruVsSpark.LocalCluster;
import exp.imruVsSpark.data.DataGenerator;
import exp.imruVsSpark.kmeans.imru.IMRUKMeans;
import exp.imruVsSpark.kmeans.spark.SparkKMeans;
import exp.test0.GnuPlot;

public class Ec2Experiments {
    String rsync = "rsync -e \"ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no\" -vrultzCc";
    LocalCluster cluster;
    HyracksNode controller;
    HyracksNode[] nodes;
    File resultDir;
    File figDir;
    static ClusterMonitor monitor;

    public Ec2Experiments(LocalCluster cluster, String name) {
        this.cluster = cluster;
        controller = cluster.cluster.controller;
        nodes = cluster.cluster.nodes;
        resultDir = new File("result/" + name + "_" + nodes.length + "nodes");
        resultDir.mkdir();
        figDir = new File(resultDir, "rawData");
        figDir.mkdir();
    }

    public static String bytesToIntString(byte[] bs) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bs.length; i++) {
            if (i > 0)
                sb.append(".");
            sb.append(bs[i] & 0xFF);
        }
        return sb.toString();
    }

    public static String getIp() throws Exception {
        for (String face : new String[] { "eth0" }) {
            NetworkInterface ni = NetworkInterface.getByName(face);
            if (ni == null)
                continue;
            Enumeration<InetAddress> addrs = ni.getInetAddresses();
            while (addrs.hasMoreElements()) {
                InetAddress inetAddress = (InetAddress) addrs.nextElement();
                byte[] bs = inetAddress.getAddress();
                if (bs.length == 16)
                    continue;
                return bytesToIntString(bs);
            }
        }
        return null;
    }

    void generateData() throws Exception {
        HyracksNode.verbose = false;
        cluster.cluster.startHyrackCluster();
        Thread.sleep(2000);
        cluster.checkHyracks();

        monitor.start(figDir, "generateData", nodes);
        PrintStream ps = new PrintStream(
                new File(resultDir, "generateTime.txt"));
        for (int aaa = EC2Benchmark.STARTC; aaa <= EC2Benchmark.ENDC; aaa += EC2Benchmark.STEPC) {
            DataGenerator.DEBUG_DATA_POINTS = aaa * EC2Benchmark.BATCH;
            int dataSize = DataGenerator.DEBUG_DATA_POINTS * nodes.length;

            long start = System.currentTimeMillis();
            Rt.p("generating data " + DataGenerator.DEBUG_DATA_POINTS + " "
                    + nodes.length);
            //            DataGenerator.main(new String[] { "/home/ubuntu/test/data.txt" });
            IMRUKMeans.generateData(controller.publicIp,
                    DataGenerator.DEBUG_DATA_POINTS, nodes.length, new File(
                            "/home/" + cluster.user
                                    + "/test/exp_data/product_name"),
                    EC2Benchmark.dataPath + "/imru" + aaa + ".txt",
                    EC2Benchmark.dataPath + "/spark" + aaa + ".txt");
            long dataTime = System.currentTimeMillis() - start;
            Rt.p(aaa + "\t" + dataTime);
            ps.println(aaa + "\t" + dataTime);
        }
        ps.close();
        monitor.stop();
        //        cluster.cluster.printLogs(-1, 100);
        //        cluster.cluster.stopHyrackCluster();
        for (HyracksNode node : nodes) {
            //            SSH ssh = node.ssh();
            //            Rt.p(node.getName());
            //            ssh.execute("ls -l -h " + EC2Benchmark.dataPath);
            //            ssh.close();
        }
    }

    void uploadExperimentCode() throws Exception {
        HyracksNode node = controller;
        SSH ssh = node.ssh();
        node.rsync(ssh, new File("/home/wangrui/ucscImru/bin"), "/home/"
                + cluster.user + "/test/bin/");
        //        node.rsync(ssh, new File("/home/wangrui/b/soft/scala-2.9.2"), "/home/"
        //                + cluster.user + "/scala-2.9.2/");
        //        node.rsync(ssh, new File("/home/wangrui/b/soft/spark-0.7.0"), "/home/"
        //                + cluster.user + "/spark-0.7.0/");
        //        node
        //                .rsync(
        //                        ssh,
        //                        new File(
        //                                "/home/wangrui/b/soft/spark-0.7.0/core/target/scala-2.9.2/classes"),
        //                        "/home/"
        //                                + cluster.user
        //                                + "/spark-0.7.0/core/target/scala-2.9.2/classes/");
        String startScript = Rt.readFile(new File(
                "/home/wangrui/ucscImru/lib/ec2runSpark.sh"));
        startScript = startScript.replaceAll("/home/ubuntu", "/home/"
                + cluster.user);
        ssh
                .put("/home/" + cluster.user + "/test/st.sh", startScript
                        .getBytes());
        node.rsync(ssh, new File("/home/wangrui/ucscImru/exp_data"), "/home/"
                + cluster.user + "/test/exp_data/");
        ssh.close();
    }

    void runImru() throws Exception {
        Rt.p("testing IMRU");
        cluster.cluster.startHyrackCluster();
        Thread.sleep(5000);
        cluster.checkHyracks();
        SSH ssh = controller.ssh();
        ssh.execute("cd test;");
        monitor.start(figDir, "imru", nodes);
        ssh.execute("sh st.sh exp.imruVsSpark.kmeans.EC2Benchmark "
                + controller.internalIp + " " + nodes.length + " true");
        String result = new String(Rt.read(ssh.get("/home/" + cluster.user
                + "/test/result/kmeansimru_org.data")));
        Rt.p(result);
        Rt.write(new File(resultDir, "imru.txt"), result.getBytes());
        ssh.close();
        monitor.stop();
        //        cluster.cluster.printLogs(-1, 100);
        //        cluster.cluster.stopHyrackCluster();
    }

    void runSpark() throws Exception {
        Rt.p("testing spark");
        cluster.startSpark();
        cluster.checkSpark();
        SSH ssh = controller.ssh();
        ssh.execute("cd test;");
        monitor.start(figDir, "spark", nodes);
        ssh.execute("sh st.sh exp.imruVsSpark.kmeans.EC2Benchmark "
                + controller.internalIp + " " + nodes.length + " false");
        monitor.stop();
        ssh.execute("cat " + "/home/" + cluster.user + "/masterSpark.log");
        ssh.execute("cat " + "/home/" + cluster.user + "/slaveSpark.log");
        String result = new String(Rt.read(ssh.get("/home/" + cluster.user
                + "/test/result/kmeansspark_org.data")));
        Rt.p(result);
        Rt.write(new File(resultDir, "spark.txt"), result.getBytes());
        //        cluster.stopSpark();
        ssh.close();
    }

    static LocalCluster getEc2Cluster(int nodeCount) throws Exception {
        File home = new File(System.getProperty("user.home"));
        File credentialsFile = new File(home, "AwsCredentials.properties");
        File privateKey = new File(home, "ruiwang.pem");
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
        return new LocalCluster(ec2.cluster, "ubuntu");
    }

    void runExperiments() throws Exception {
        Rt.p("Spark: http://" + controller.publicIp + ":"
                + cluster.getSparkPort() + "/");
        Rt.p("IMRU: " + cluster.cluster.getAdminURL());
        cluster.stopAll();

        uploadExperimentCode();
        generateData();

        cluster.stopAll();
        runImru();

        cluster.stopAll();
        runSpark();

        cluster.stopAll();
    }

    public static void main(String[] args) throws Exception {
        try {
            monitor = new ClusterMonitor();
            String[] nodes = new String[8];
            if (nodes.length == 1) {
                nodes = new String[] { "192.168.56.110" };
            } else {
                monitor.waitIp(nodes.length);
                for (int i = 0; i < nodes.length; i++)
                    nodes[i] = monitor.ip[i];
            }

            File home = new File(System.getProperty("user.home"));
            File hyracksEc2Root = new File(home, "ucscImru/dist");
            LocalCluster cluster;
            String name;
            String userName = "ubuntu";

            HyracksNode.HYRACKS_PATH = "/home/" + userName + "/hyracks-ec2";
            name = "local2G0.5core";
            String cc = nodes[0];
            cluster = new LocalCluster(new HyracksCluster(cc, nodes, userName,
                    new File(home, ".ssh/id_rsa")), userName);
            //        cluster.cluster.install(hyracksEc2Root);
            Ec2Experiments exp = new Ec2Experiments(cluster, name);
            if (nodes.length == 1)
                exp.uploadExperimentCode();
            else
                exp.runExperiments();
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            System.exit(0);
        }
    }
}
