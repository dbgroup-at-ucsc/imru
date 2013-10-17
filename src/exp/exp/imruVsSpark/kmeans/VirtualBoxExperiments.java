package exp.imruVsSpark.kmeans;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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
import edu.uci.ics.hyracks.imru.dataflow.IMRUDebugger;
import edu.uci.ics.hyracks.imru.example.utils.Client;
import edu.uci.ics.hyracks.imru.example.utils.ImruEC2;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.ClusterMonitor;
import exp.ImruDebugMonitor;
import exp.imruVsSpark.LocalCluster;
import exp.imruVsSpark.VirtualBox;
import exp.imruVsSpark.data.DataGenerator;
import exp.imruVsSpark.kmeans.imru.IMRUKMeans;
import exp.imruVsSpark.kmeans.spark.SparkKMeans;
import exp.test0.GnuPlot;

public class VirtualBoxExperiments {
    String rsync = "rsync -e \"ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no\" -vrultzCc";
    LocalCluster cluster;
    HyracksNode controller;
    HyracksNode[] nodes;
    int k;
    int iterations;
    int batchStart;
    int batchStep;
    int batchEnd;
    int batchSize;
    String aggType;
    int aggArg;
    File resultDir;
    File figDir;
    static ClusterMonitor monitor;

    public VirtualBoxExperiments(LocalCluster cluster, String name, int k,
            int iterations, int batchStart, int batchStep, int batchEnd,
            int batchSize, String aggType, int aggArg) {
        this.k = k;
        this.iterations = iterations;
        this.batchStart = batchStart;
        this.batchStep = batchStep;
        this.batchEnd = batchEnd;
        this.batchSize = batchSize;
        this.aggType = aggType;
        this.aggArg = aggArg;
        this.cluster = cluster;
        if (cluster != null) {
            controller = cluster.cluster.controller;
            nodes = cluster.cluster.nodes;
            resultDir = new File("result/k" + k + "i" + iterations + "b"
                    + batchStart + "s" + batchStep + "e" + batchEnd + "b"
                    + batchSize + "/" + name + "_" + nodes.length + "nodes_"
                    + aggType + "_" + aggArg);
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
        for (int sizePerNode = batchStart; sizePerNode <= batchEnd; sizePerNode += batchStep) {
            int pointPerNode = sizePerNode * batchSize;
            int dataSize = pointPerNode * nodes.length;

            long start = System.currentTimeMillis();
            Rt.p("generating data " + pointPerNode + " " + nodes.length);
            //            DataGenerator.main(new String[] { "/home/ubuntu/test/data.txt" });
            IMRUKMeans.generateData(controller.publicIp, pointPerNode,
                    nodes.length, new File("/home/" + cluster.user
                            + "/test/exp_data/product_name"), KmeansExperiment
                            .getImruDataPath(sizePerNode, nodes.length, "%d"),
                    KmeansExperiment
                            .getSparkDataPath(sizePerNode, nodes.length));
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
            //            ssh.execute("ls -l -h " + KmeansExperiment.dataPath);
            //            ssh.close();
        }
    }

    void generateSharedData() throws Exception {
        for (int sizePerNode = batchStart; sizePerNode <= batchEnd; sizePerNode += batchStep) {
            int pointPerNode = sizePerNode * batchSize;
            int dataSize = pointPerNode * nodes.length;
            String imruPath = "/data"
                    + KmeansExperiment.getImruDataPath(sizePerNode,
                            nodes.length, "%d");
            String sparkPath = "/data"
                    + KmeansExperiment.getSparkDataPath(sizePerNode,
                            nodes.length);
            int count = pointPerNode;
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

    static void uploadExperimentCode(LocalCluster cluster, boolean isTemplate)
            throws Exception {
        HyracksNode node = cluster.cluster.controller;
        SSH ssh = node.ssh();
        if (isTemplate) {
            node.rsync(ssh, new File("/home/wangrui/ucscImru/dist/lib"),
                    "/home/" + cluster.user + "/hyracks-ec2/lib/");
            node.rsync(ssh, new File("/home/wangrui/ucscImru/dist/bin"),
                    "/home/" + cluster.user + "/hyracks-ec2/bin/");
            node.rsync(ssh, new File("/home/wangrui/ucscImru/bin/scripts"),
                    "/home/" + cluster.user + "/hyracks-ec2/bin/");
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
        }
        node.rsync(ssh, new File("/home/wangrui/ucscImru/bin"), "/home/"
                + cluster.user + "/test/bin/");
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

    void runExperiment(SSH ssh, String job) {
        String arg = "-master " + controller.internalIp;
        arg += " -nodeCount " + nodes.length;
        arg += " -type " + job;
        arg += " -k " + k;
        arg += " -iterations " + iterations;
        arg += " -batchStart " + batchStart;
        arg += " -batchStep " + batchStep;
        arg += " -batchEnd " + batchEnd;
        arg += " -batchSize " + batchSize;
        arg += " -agg-tree-type " + aggType;
        arg += " -agg-count " + aggArg;
        arg += " -fan-in " + aggArg;
        ssh.execute("sh st.sh exp.imruVsSpark.kmeans.KmeansExperiment " + arg);
    }

    boolean hasResult(boolean mem) throws Exception {
        String job = mem ? "imruMem" : "imruDisk";
        File resultFile = new File(resultDir, job + ".txt");
        return (resultFile.exists() && resultFile.length() > 0);
    }

    boolean hasSparkResult() throws Exception {
        File resultFile = new File(resultDir, "spark.txt");
        return (resultFile.exists() && resultFile.length() > 0);
    }

    void runImru(boolean mem) throws Exception {
        String job = mem ? "imruMem" : "imruDisk";
        File resultFile = new File(resultDir, job + ".txt");
        //        if (resultFile.exists() && resultFile.length() > 0)
        //            return;
        cluster.stopAll();
        Rt.p("testing IMRU");
        cluster.cluster.startHyrackCluster();
        Thread.sleep(5000);
        cluster.checkHyracks();
        SSH ssh = controller.ssh();
        ssh.execute("cd test;");
        monitor.start(figDir, job, nodes);
        ssh.execute("rm result/*");
        runExperiment(ssh, job);
        String result = new String(Rt.read(ssh.get("/home/" + cluster.user
                + "/test/result/kmeans" + job + "_org.data")));
        Rt.p(result);
        Rt.write(resultFile, result.getBytes());
        ssh.close();
        monitor.stop();
        //        cluster.cluster.printLogs(-1, 100);
        //        cluster.cluster.stopHyrackCluster();
    }

    void runSpark() throws Exception {
        if (hasSparkResult())
            return;
        cluster.stopAll();
        Rt.p("testing spark");
        cluster.startSpark();
        cluster.checkSpark();
        SSH ssh = controller.ssh();
        ssh.execute("cd test;");
        monitor.start(figDir, "spark", nodes);
        ssh.execute("rm result/*");
        runExperiment(ssh, "spark");
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
        for (File group : new File("result").listFiles()) {
            if (!group.isDirectory())
                continue;
            if (!group.getName().startsWith("k"))
                continue;
            for (File dir : group.listFiles()) {
                if (dir.isDirectory() && dir.getName().startsWith("local")) {
                    if (!new File(dir, "spark.txt").exists())
                        continue;
                    generateResult(dir);
                }
            }
        }
        System.exit(0);
    }

    static void generateResult(File resultDir) throws Exception {
        KmeansFigs figs = new KmeansFigs(resultDir);
        GnuPlot plot = new GnuPlot(new File("/tmp/cache"), "kmeans",
                "Data points per node (10^5)", "Time (seconds)");
        GnuPlot speedup = new GnuPlot(new File("/tmp/cache"), "kmeansSpeedup",
                "Data points per node (10^5)", "Speed up (%)");
        plot.extra = "set title \"K-means K=" + figs.k + " Iteration="
                + figs.iterations + "\\n" + " mem=" + figs.memory + "M*"
                + figs.nodeCount + " cpu=" + figs.core + "core*"
                + figs.nodeCount + "\"";
        speedup.extra = "set title \"K-means K=" + figs.k + " Iteration="
                + figs.iterations + "\\n" + " mem=" + figs.memory + "M*"
                + figs.nodeCount + " cpu=" + figs.core + "core*"
                + figs.nodeCount + "\"";
        plot.setPlotNames(
        //                "Generate Data", 
                "Spark", "IMRU-disk", "IMRU-mem");
        speedup.setPlotNames("IMRU-mem vs Spark", "IMRU-disk vs Spark");
        plot.startPointType = 1;
        plot.pointSize = 1;
        plot.scale = false;
        plot.colored = true;
        plot.keyPosition = "left top";
        speedup.startPointType = 1;
        speedup.pointSize = 1;
        speedup.scale = false;
        speedup.colored = true;
        speedup.keyPosition = "left top";

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

            String[] ss4 = imruDisk[i].split("\t");
            if (!ss4[0].equals(dataSizes[i]))
                throw new Error();
            double imruDiskTime = Double.parseDouble(ss4[1]);
            plot.addY(imruDiskTime);
            if (!ss4[2].equals(processed[i]))
                throw new Error();

            String[] ss3 = imruMem[i].split("\t");
            if (!ss3[0].equals(dataSizes[i]))
                throw new Error();
            double imruMemTime = Double.parseDouble(ss3[1]);
            plot.addY(imruMemTime);
            if (!ss3[2].equals(processed[i]))
                throw new Error();

            speedup.addY(sparkTime / imruMemTime * 100 - 100);
            speedup.addY(sparkTime / imruDiskTime * 100 - 100);
        }

        plot.finish();
        speedup.finish();

        String prefix = "../finished/"
                + figs.name.replaceAll("_", "").replaceAll("\\.", "");
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
        if (hasResult(true) && hasResult(false) && hasSparkResult())
            return;
        Rt.p("Spark: http://" + controller.publicIp + ":"
                + cluster.getSparkPort() + "/");
        Rt.p("IMRU: " + cluster.cluster.getAdminURL());

        uploadExperimentCode(cluster, false);
        generateSharedData();

        runImru(true);

        runImru(false);

        runSpark();

        cluster.stopAll();
    }

    void runIMRUMem() throws Exception {
        Rt.p("IMRU: " + cluster.cluster.getAdminURL());

        uploadExperimentCode(cluster, false);
        generateSharedData();

        runImru(true);

        cluster.stopAll();
    }

    static void createTemplate(String ip, String userName) throws Exception {
        //                VirtualBox.remove();
        //                System.exit(0);
        File home = new File(System.getProperty("user.home"));
        String[] nodes = new String[] { ip };
        String cc = nodes[0];
        LocalCluster cluster = new LocalCluster(new HyracksCluster(cc, nodes,
                userName, new File(home, ".ssh/id_rsa")), userName);
        uploadExperimentCode(cluster, true);
        System.exit(0);
    }

    public static void runExp(String[] args) throws Exception {
        //        generateResult(new File(
        //                "result/k3i1b1s3e10b100000/local1500M0.25core_16nodes"));
        //        generateResult(new File(
        //                "result/k3i1b1s3e10b100000/local1500M0.5core_8nodes"));
        //        System.exit(0);
        //        regenerateResults();
        try {
            VirtualBox.remove();
            //            System.exit(0);
            int nodeCount = 16;
            int memory = 1500;
            int k = 3;
            int iterations = 5;
            int batchStart = 1;
            int batchStep = 3;
            int batchEnd = 1;
            int batchSize = 100000;
            int network = 0;
            String cpu = "0.25";
            int fanIn = 2;

            nodeCount = 12;
            memory = 1500;
            cpu = "0.25";

            nodeCount = 2;
            memory = 2000;
            cpu = "0.5";
            iterations = 1;
            fanIn = 2;

            //            nodeCount = 16;
            //            memory = 1500;
            //            cpu = "0.25";
            //            iterations = 1;
            //            fanIn = 2;
            //            network = 1; 

            //            for (fanIn = 0; fanIn <= 6; fanIn++) {
            //                if (fanIn == 1)
            //                    continue;
            for (k = 1; k <= 1; k++) {
                //                File outputFile = new File("result/fan16_" + fanIn + "_" + k
                //                        + ".txt");
                //                if (outputFile.exists() && outputFile.length() > 0)
                //                    continue;
                //                        for (k = 16; k <= 64; k *= 2) {
                VirtualBox.setup(nodeCount, memory, (int) (Double
                        .parseDouble(cpu) * 100), network);
                Thread.sleep(2000 * nodeCount);
                monitor = new ClusterMonitor();
                String[] nodes = new String[nodeCount];
                monitor.waitIp(nodes.length);
                for (int i = 0; i < nodes.length; i++) {
                    nodes[i] = monitor.ip[i];
                    System.out.println("NC" + i + ": " + nodes[i]);
                }
                //            for (network = 1; network <= 5; network *= 10) {
                //            for (k = 1; k <= 10; k++) {
                //                for (fanIn = 1; fanIn <= 5; fanIn++) {

                String name = "local" + memory + "M" + cpu + "coreN" + network;

                File home = new File(System.getProperty("user.home"));
                LocalCluster cluster;
                String userName = "ubuntu";

                HyracksNode.HYRACKS_PATH = "/home/" + userName + "/hyracks-ec2";
                String cc = nodes[0];
                cluster = new LocalCluster(new HyracksCluster(cc, nodes,
                        userName, new File(home, ".ssh/id_rsa")), userName);
                //                File hyracksEc2Root = new File(home, "ucscImru/dist");
                //        cluster.cluster.install(hyracksEc2Root);
                VirtualBoxExperiments exp = new VirtualBoxExperiments(cluster,
                        name, k, iterations, batchStart, batchStep, batchEnd,
                        batchSize, fanIn > 1 ? "nary" : "none", fanIn);
                //            exp.runExperiments();

                //                IMRUDebugger.debug = true;
                //                ImruDebugMonitor monitor = new ImruDebugMonitor(outputFile
                //                        .getAbsolutePath());
                exp.runIMRUMem();
                //                monitor.close();
                //                    generateResult(exp.resultDir);

                //                }
                //            }
                VirtualBox.remove();
                VirtualBoxExperiments.monitor.close();
            }
            //            }

        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            System.exit(0);
        }
    }

    public static void main(String[] args) throws Exception {
        //        createTemplate("192.168.56.110", "ubuntu");
        runExp(args);
    }
}
