package exp.imruVsSpark.kmeans;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
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
import exp.imruVsSpark.VirtualBox;
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
        if (cluster != null) {
            controller = cluster.cluster.controller;
            nodes = cluster.cluster.nodes;
            resultDir = new File("result/k" + DataGenerator.DEBUG_K + "i"
                    + DataGenerator.DEBUG_ITERATIONS + "b"
                    + EC2Benchmark.STARTC + "s" + EC2Benchmark.STEPC + "e"
                    + EC2Benchmark.ENDC + "b" + EC2Benchmark.BATCH + "/" + name
                    + "_" + nodes.length + "nodes");
            resultDir.mkdirs();
        }
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
        for (int sizePerNode = EC2Benchmark.STARTC; sizePerNode <= EC2Benchmark.ENDC; sizePerNode += EC2Benchmark.STEPC) {
            DataGenerator.DEBUG_DATA_POINTS = sizePerNode * EC2Benchmark.BATCH;
            int dataSize = DataGenerator.DEBUG_DATA_POINTS * nodes.length;

            long start = System.currentTimeMillis();
            Rt.p("generating data " + DataGenerator.DEBUG_DATA_POINTS + " "
                    + nodes.length);
            //            DataGenerator.main(new String[] { "/home/ubuntu/test/data.txt" });
            IMRUKMeans.generateData(controller.publicIp,
                    DataGenerator.DEBUG_DATA_POINTS, nodes.length, new File(
                            "/home/" + cluster.user
                                    + "/test/exp_data/product_name"),
                    EC2Benchmark.getImruDataPath(sizePerNode, nodes.length,
                            "%d"), EC2Benchmark.getSparkDataPath(sizePerNode,
                            nodes.length));
            long dataTime = System.currentTimeMillis() - start;
            Rt.p(sizePerNode + "\t" + dataTime / 1000.0);
            ps.println(sizePerNode + "\t" + dataTime / 1000.0);
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

    void generateSharedData() throws Exception {
        for (int sizePerNode = EC2Benchmark.STARTC; sizePerNode <= EC2Benchmark.ENDC; sizePerNode += EC2Benchmark.STEPC) {
            DataGenerator.DEBUG_DATA_POINTS = sizePerNode * EC2Benchmark.BATCH;
            int dataSize = DataGenerator.DEBUG_DATA_POINTS * nodes.length;
            String imruPath = "/data"
                    + EC2Benchmark.getImruDataPath(sizePerNode, nodes.length,
                            "%d");
            String sparkPath = "/data"
                    + EC2Benchmark.getSparkDataPath(sizePerNode, nodes.length);
            int count = DataGenerator.DEBUG_DATA_POINTS;
            int splits = nodes.length;
            File templateDir = new File("exp_data/product_name");
            File sparkFile = new File(sparkPath);
            if (sparkFile.exists())
                continue;
            sparkFile.getParentFile().mkdirs();
            PrintStream psSpark = new PrintStream(new BufferedOutputStream(
                    new FileOutputStream(sparkFile), 1024 * 1024));
            DataGenerator dataGenerator = new DataGenerator(dataSize,
                    templateDir);
            for (int i = 0; i < splits; i++) {
                File imruFile = new File(String.format(imruPath, i));
                PrintStream psImru = new PrintStream(new BufferedOutputStream(
                        new FileOutputStream(imruFile), 1024 * 1024));
                dataGenerator.generate(false, count, psSpark, psImru);
                psImru.close();
            }
            psSpark.close();
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

    void runImru(boolean mem) throws Exception {
        String job = mem ? "imruMem" : "imruDisk";
        Rt.p("testing IMRU");
        cluster.cluster.startHyrackCluster();
        Thread.sleep(5000);
        cluster.checkHyracks();
        SSH ssh = controller.ssh();
        ssh.execute("cd test;");
        monitor.start(figDir, job, nodes);
        ssh.execute("rm result/*");
        ssh.execute("sh st.sh exp.imruVsSpark.kmeans.EC2Benchmark "
                + controller.internalIp + " " + nodes.length + " " + job);
        String result = new String(Rt.read(ssh.get("/home/" + cluster.user
                + "/test/result/kmeans" + job + "_org.data")));
        Rt.p(result);
        Rt.write(new File(resultDir, job + ".txt"), result.getBytes());
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
        ssh.execute("rm result/*");
        ssh.execute("sh st.sh exp.imruVsSpark.kmeans.EC2Benchmark "
                + controller.internalIp + " " + nodes.length + " spark");
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

    static void regenerateResults() throws Exception {
        for (File dir : new File("result/k3i5b1s3e10b100000").listFiles()) {
            if (dir.isDirectory() && dir.getName().startsWith("local")) {
                if (!new File(dir, "spark.txt").exists())
                    continue;
                generateResult(dir);
            }
        }
        System.exit(0);
    }

    static void generateResult(File resultDir) throws Exception {
        String name = resultDir.getName();
        int memory = Integer.parseInt(name.substring(5, name.indexOf("M")));
        String core = name.substring(name.indexOf("M") + 1, name
                .indexOf("core"));
        int nodeCount = Integer.parseInt(name.substring(
                name.lastIndexOf("_") + 1, name.length() - 5));
        GnuPlot plot = new GnuPlot(new File("/tmp/cache"), "kmeans",
                "Data points per node (10^5)", "Time (seconds)");
        GnuPlot speedup = new GnuPlot(new File("/tmp/cache"), "kmeansSpeedup",
                "Data points per node (10^5)", "Speed up (%)");
        plot.extra = "set title \"K-means K=" + DataGenerator.DEBUG_K
                + " Iteration=" + DataGenerator.DEBUG_ITERATIONS + "\\n"
                + " mem=" + memory + "M*" + nodeCount + " cpu=" + core
                + "core*" + nodeCount + "\"";
        speedup.extra = "set title \"K-means K=" + DataGenerator.DEBUG_K
                + " Iteration=" + DataGenerator.DEBUG_ITERATIONS + "\\n"
                + " mem=" + memory + "M*" + nodeCount + " cpu=" + core
                + "core*" + nodeCount + "\"";
        plot.setPlotNames(
        //                "Generate Data", 
                "Spark", "IMRU-mem", "IMRU-disk");
        speedup.setPlotNames("IMRU-mem vs Spark", "IMRU-disk vs Spark");
        plot.startPointType = 1;
        plot.pointSize = 1;
        plot.scale = false;
        speedup.startPointType = 1;
        speedup.pointSize = 1;
        speedup.scale = false;

        //        String[] data = Rt.readFile(new File(resultDir, "generateTime.txt"))
        //                .split("\n");
        String[] imruDisk = Rt.readFile(new File(resultDir, "imruDisk.txt"))
                .split("\n");
        String[] imruMem = Rt.readFile(new File(resultDir, "imruMem.txt"))
                .split("\n");
        String[] spark = Rt.readFile(new File(resultDir, "spark.txt")).split(
                "\n");
        String[] dataSizes = new String[imruDisk.length];
        String[] processed = new String[imruDisk.length];
        for (int i = 0; i < imruDisk.length; i++) {
            String[] ss1 = imruDisk[i].split("\t");
            double dataSize = Double.parseDouble(ss1[0]);
            dataSizes[i] = ss1[0];
            plot.startNewX(dataSize);
            speedup.startNewX(dataSize);
            //            plot.addY(Double.parseDouble(ss1[1]));

            String[] ss2 = spark[i].split("\t");
            if (!ss2[0].equals(dataSizes[i]))
                throw new Error(spark[i] + " " + imruDisk[i]);
            double sparkTime = Double.parseDouble(ss2[1]);
            plot.addY(sparkTime);
            processed[i] = ss2[2];

            String[] ss3 = imruMem[i].split("\t");
            if (!ss3[0].equals(dataSizes[i]))
                throw new Error();
            double imruMemTime = Double.parseDouble(ss3[1]);
            plot.addY(imruMemTime);
            if (!ss3[2].equals(processed[i]))
                throw new Error();

            String[] ss4 = imruDisk[i].split("\t");
            if (!ss4[0].equals(dataSizes[i]))
                throw new Error();
            double imruDiskTime = Double.parseDouble(ss4[1]);
            plot.addY(imruDiskTime);
            if (!ss4[2].equals(processed[i]))
                throw new Error();

            speedup.addY(sparkTime / imruMemTime * 100 - 100);
            speedup.addY(sparkTime / imruDiskTime * 100 - 100);
        }

        plot.finish();
        speedup.finish();

        String prefix = "../finished/"
                + name.replaceAll("_", "").replaceAll("\\.", "");
        File pdf = new File(resultDir, prefix + plot.name + ".pdf");
        if (!pdf.getParentFile().exists())
            pdf.getParentFile().mkdirs();
        String cmd = "epstopdf --outfile=" + pdf.getAbsolutePath() + " "
                + new File(plot.dir, plot.name + ".eps").getAbsolutePath();
        Rt.runAndShowCommand(cmd);
        cmd = "epstopdf --outfile="
                + new File(resultDir, prefix + speedup.name + ".pdf")
                        .getAbsolutePath()
                + " "
                + new File(speedup.dir, speedup.name + ".eps")
                        .getAbsolutePath();
        Rt.runAndShowCommand(cmd);
        //        plot.show();
    }

    void runExperiments() throws Exception {
        Rt.p("Spark: http://" + controller.publicIp + ":"
                + cluster.getSparkPort() + "/");
        Rt.p("IMRU: " + cluster.cluster.getAdminURL());

        uploadExperimentCode();
        generateSharedData();

        cluster.stopAll();
        runImru(true);

        cluster.stopAll();
        runImru(false);

        cluster.stopAll();
        runSpark();

        cluster.stopAll();
    }

    public static void main(String[] args) throws Exception {
        //        regenerateResults();
        try {
            String name;
            int nodeCount;

            name = "local1500M0.25core";
            nodeCount = 16;

            nodeCount = 8;
            String cpu = "0.5";
            name = "local1500M0.5core";
            for (int memory = 2000; memory <= 2500; memory += 500) {
                name = "local" + memory + "M" + cpu + "core";

                VirtualBox.remove();
                VirtualBox.setup(nodeCount, memory, (int) (Double
                        .parseDouble(cpu) * 100));
                Thread.sleep(30000);
                monitor = new ClusterMonitor();
                String[] nodes = new String[nodeCount];
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
                String userName = "ubuntu";

                HyracksNode.HYRACKS_PATH = "/home/" + userName + "/hyracks-ec2";
                String cc = nodes[0];
                cluster = new LocalCluster(new HyracksCluster(cc, nodes,
                        userName, new File(home, ".ssh/id_rsa")), userName);
                //            cluster.cluster.printLogs(-1, 500);
                //            System.exit(0);
                //        cluster.cluster.install(hyracksEc2Root);
                Ec2Experiments exp = new Ec2Experiments(cluster, name);
                if (nodes.length == 1)
                    exp.uploadExperimentCode();
                else
                    exp.runExperiments();
                generateResult(exp.resultDir);
            }
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            System.exit(0);
        }
    }
}
