package exp;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

import scala.actors.threadpool.Arrays;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.ec2.HyracksCluster;
import edu.uci.ics.hyracks.ec2.HyracksEC2Cluster;
import edu.uci.ics.hyracks.ec2.HyracksEC2Node;
import edu.uci.ics.hyracks.ec2.HyracksNode;
import edu.uci.ics.hyracks.ec2.NodeCallback;
import edu.uci.ics.hyracks.ec2.SSH;
import edu.uci.ics.hyracks.imru.dataflow.IMRUDebugger;
import edu.uci.ics.hyracks.imru.example.utils.ImruEC2;
import edu.uci.ics.hyracks.imru.util.Client;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.experiments.DataPointsPerNode;
import exp.experiments.FanInAndK;
import exp.imruVsSpark.LocalCluster;
import exp.imruVsSpark.VirtualBox;
import exp.imruVsSpark.data.DataGenerator;
import exp.imruVsSpark.kmeans.imru.IMRUKMeans;
import exp.imruVsSpark.kmeans.spark.SparkKMeans;
import exp.test0.GnuPlot;
import exp.types.ImruExpParameters;
import exp.types.ImruExperimentTimeoutException;

public class VirtualBoxExperiments {
    String rsync = "rsync -e \"ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no\" -vrultzCc";
    LocalCluster cluster;
    HyracksNode controller;
    HyracksNode[] nodes;
    ImruExpParameters p;
    File resultDir;
    File figDir;
    public static ClusterMonitor monitor;

    //    public static boolean MONITOR_MEMORY_USAGE = true;
    //    public static int MAX_NODES_STARTUP_TIME = 5 * 60 * 1000;
    //    public static int MAX_EXPERIMENT_FREEZE_TIME = 30 * 60 * 1000;

    public VirtualBoxExperiments(LocalCluster cluster, ImruExpParameters p)
            throws IOException {
        this.p = p;
        if (p.experiment == null)
            throw new Error();
        this.cluster = cluster;
        if (cluster != null) {
            controller = cluster.cluster.controller;
            nodes = cluster.cluster.nodes;
            resultDir = p.getResultFolder();
            resultDir.mkdirs();
            Rt.write(new File(resultDir, "parameters.obj"), p.toByteArray());
            Rt.write(new File(resultDir, "parameters.txt"), p.toString()
                    .getBytes());
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

    //    void generateData() throws Exception {
    //        HyracksNode.verbose = false;
    //        cluster.cluster.startHyrackCluster();
    //        Thread.sleep(2000);
    //        cluster.checkHyracks();
    //
    //        if (MONITOR_MEMORY_USAGE)
    //            monitor.start(figDir, "generateData", nodes);
    //        PrintStream ps = new PrintStream(
    //                new File(resultDir, "generateTime.txt"));
    //        for (int sizePerNode = batchStart; sizePerNode <= batchEnd; sizePerNode += batchStep) {
    //            int pointPerNode = sizePerNode * batchSize;
    //            int dataSize = pointPerNode * nodes.length;
    //
    //            long start = System.currentTimeMillis();
    //            Rt.p("generating data " + pointPerNode + " " + nodes.length);
    //            //            DataGenerator.main(new String[] { "/home/ubuntu/test/data.txt" });
    //            IMRUKMeans.generateData(controller.publicIp, pointPerNode,
    //                    nodes.length, new File("/home/" + cluster.user
    //                            + "/test/exp_data/product_name"), KmeansExperiment
    //                            .getImruDataPath(sizePerNode, nodes.length, "%d"),
    //                    KmeansExperiment.getDataPath(sizePerNode, nodes.length));
    //            long dataTime = System.currentTimeMillis() - start;
    //            Rt.p(sizePerNode + "\t" + dataTime / 1000.0);
    //            ps.println(sizePerNode + "\t" + dataTime / 1000.0);
    //        }
    //        ps.close();
    //        if (MONITOR_MEMORY_USAGE)
    //            monitor.stop();
    //        //        cluster.cluster.printLogs(-1, 100);
    //        //        cluster.cluster.stopHyrackCluster();
    //        for (HyracksNode node : nodes) {
    //            //            SSH ssh = node.ssh();
    //            //            Rt.p(node.getName());
    //            //            ssh.execute("ls -l -h " + KmeansExperiment.dataPath);
    //            //            ssh.close();
    //        }
    //    }

    void generateSharedData() throws Exception {
        for (int sizePerNode = p.batchStart; sizePerNode <= p.batchEnd; sizePerNode += p.batchStep) {
            int pointPerNode = sizePerNode * p.batchSize;
            int dataSize = pointPerNode * nodes.length;
            String dataPath = "/data"
                    + ElasticImruExperimentEntry.getDataPath(p.batchSize,
                            sizePerNode, nodes.length, p.numOfDimensions);
            int count = pointPerNode;
            int splits = nodes.length;
            File templateDir = new File("exp_data/product_name");
            File dataFile = new File(dataPath);
            if (dataFile.exists())
                continue;
            dataFile.getParentFile().mkdirs();
            PrintStream ps = new PrintStream(new BufferedOutputStream(
                    new FileOutputStream(dataFile), 1024 * 1024));
            DataGenerator dataGenerator = new DataGenerator(dataSize,
                    p.numOfDimensions, templateDir);
            for (int i = 0; i < splits; i++) {
                File infoFile = new File(dataPath + ".dims");
                dataGenerator.generate(true, count, ps, infoFile);
            }
            ps.close();
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
            node.rsync(ssh, new File("/home/wangrui/b/soft/scala-2.9.3"),
                    "/home/" + cluster.user + "/scala-2.9.3/");
            node.rsync(ssh, new File(
                    "/home/wangrui/b/soft/spark-0.8.0-incubating"), "/home/"
                    + cluster.user + "/spark-0.8.0-incubating/");
            node.rsync(ssh, new File("/home/wangrui/b/soft/stratosphere"),
                    "/home/" + cluster.user + "/stratosphere/");
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

    boolean runExperiment(SSH ssh, String job) throws Exception {
        p.master = controller.internalIp;
        p.nodeCount = nodes.length;
        p.method = job;
        ssh.maxFreezeTime = p.maxExperimentFreezeTime;
        String parameterPath = "/tmp/imruExpArgs.bin";
        try {
            ssh.put(parameterPath, p.toByteArray());
            ssh.execute("sh st.sh exp.ElasticImruExperimentEntry "
                    + parameterPath);
        } catch (IOException e) {
            if (!e.getMessage().startsWith("timeout"))
                throw e;
            Rt.p(e.getMessage());
            return false;
        }
        return true;
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
        if (resultFile.exists() && resultFile.length() > 0)
            return;
        cluster.stopAll();
        Rt.p("testing IMRU");
        cluster.cluster.startHyrackCluster();
        Thread.sleep(5000);
        cluster.checkHyracks();
        SSH ssh = controller.ssh();
        ssh.execute("cd test;");
        if (p.monitorMemoryUsage)
            monitor.start(figDir, job, nodes);
        ssh.execute("rm " + p.resultFolder + "/*");
        if (runExperiment(ssh, job)) {
            try {
                String result = new String(Rt.read(ssh.get("/home/"
                        + cluster.user + "/test/result/kmeans" + job
                        + "_org.data")));
                Rt.p(result);
                Rt.write(resultFile, result.getBytes());
            } catch (Exception e) {
                e.printStackTrace();
            }
            for (int i = 0; i < p.iterations; i++) {
                try {
                    String result = new String(Rt.read(ssh.get("/home/"
                            + cluster.user + "/test/result/" + i + ".log")));
                    Rt.write(new File(resultDir, job + "_" + i + ".log"),
                            result.getBytes());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            for (int i = 0; i < nodes.length; i++) {
                try {
                    String result = cluster.cluster.getNode(i).getLog(ssh,
                            false, 100);
                    Rt.write(new File(resultDir, job + "_NC" + i + ".log"),
                            result.getBytes());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        String result = cluster.cluster.getNode(0).getLog(ssh, true, 100);
        Rt.write(new File(resultDir, job + "_CC.log"), result.getBytes());
        ssh.close();
        if (p.monitorMemoryUsage)
            monitor.stop();
        //        System.exit(0);
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
        if (p.monitorMemoryUsage)
            monitor.start(figDir, "spark", nodes);
        ssh.execute("rm " + p.resultFolder + "/*");
        if (runExperiment(ssh, "spark")) {
            ssh.execute("cat " + "/home/" + cluster.user + "/masterSpark.log");
            ssh.execute("cat " + "/home/" + cluster.user + "/slaveSpark.log");
            String result = new String(Rt.read(ssh.get("/home/" + cluster.user
                    + "/test/result/kmeansspark_org.data")));
            Rt.p(result);
            Rt.write(new File(resultDir, "spark.txt"), result.getBytes());
            //        cluster.stopSpark();
            ssh.close();
        }
        if (p.monitorMemoryUsage)
            monitor.stop();
    }

    void runStratosphere() throws Exception {
        //        if (hasSparkResult())
        //            return;
        cluster.stopAll();
        Rt.p("testing stratosphere");
        cluster.startStratosphere();
        Thread.sleep(20000);
        SSH ssh = controller.ssh();
        ssh.execute("cd test;");
        if (p.monitorMemoryUsage)
            monitor.start(figDir, "spark", nodes);
        ssh.execute("rm " + p.resultFolder + "/*");
        cluster.cluster.executeOnAllNode(new NodeCallback() {
            @Override
            public void run(HyracksNode node) throws Exception {
                Rt.p("creating model for " + node.name);
                SSH ssh = node.ssh();
                ssh.put("/tmp/stratosphere_tmp.txt", "abc".getBytes());
                ssh.close();
            }
        });
        runExperiment(ssh, "stratosphere");
        if (p.monitorMemoryUsage)
            monitor.stop();
        //        ssh.execute("cat " + "/home/" + cluster.user + "/masterSpark.log");
        //        ssh.execute("cat " + "/home/" + cluster.user + "/slaveSpark.log");
        String result = new String(Rt.read(ssh.get("/home/" + cluster.user
                + "/test/result/kmeansstratosphere_org.data")));
        Rt.p(result);
        Rt.write(new File(resultDir, "stratosphere.txt"), result.getBytes());
        cluster.stopStratosphere();
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

    static void generateResult(File resultDir) throws Exception {
        ImruExpFigs figs = new ImruExpFigs(resultDir);
        GnuPlot plot = new GnuPlot(new File("/tmp/cache"), "kmeans",
                "Data points per node (10^5)", "Time (seconds)");
        GnuPlot speedup = new GnuPlot(new File("/tmp/cache"), "kmeansSpeedup",
                "Data points per node (10^5)", "Speed up (%)");
        plot.extra = "set title \"K-means K=" + figs.p.k + " Iteration="
                + figs.p.iterations + "\\n" + " mem=" + figs.p.memory + "M*"
                + figs.p.nodeCount + " cpu=" + figs.p.cpu + "core*"
                + figs.p.nodeCount + "\"";
        speedup.extra = "set title \"K-means K=" + figs.p.k + " Iteration="
                + figs.p.iterations + "\\n" + " mem=" + figs.p.memory + "M*"
                + figs.p.nodeCount + " cpu=" + figs.p.cpu + "core*"
                + figs.p.nodeCount + "\"";
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

    public boolean hasAllResults() throws Exception {
        return hasResult(true) && hasResult(false) && hasSparkResult();
    }

    public void runExperiments() throws Exception {
        if (hasResult(true) && hasResult(false) && hasSparkResult())
            return;
        Rt.p("Spark: http://" + controller.publicIp + ":"
                + cluster.getSparkPort() + "/");
        Rt.p("IMRU: " + cluster.cluster.getAdminURL());

        uploadExperimentCode(cluster, false);
        generateSharedData();

        try {
            runImru(true);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        try {
            runImru(false);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        try {
            runSpark();
        } catch (Throwable e) {
            e.printStackTrace();
        }

        cluster.stopAll();
    }

    public void runIMRUMem() throws Exception {
        if (hasResult(true))
            return;
        Rt.p("IMRU: " + cluster.cluster.getAdminURL());

        uploadExperimentCode(cluster, false);
        generateSharedData();

        try {
            runImru(true);
        } catch (Exception e) {
            e.printStackTrace();
        }

        cluster.stopAll();
    }

    public static boolean IMRU_ONLY = false;
    private static int lastNodeCount = 0;
    private static int lastMemory = 0;
    private static String lastCpu = null;
    private static int lastNetwork = 0;

    public static String[] startNodes(int nodeCount, int memory, String cpu,
            int network, int maxStartTime) throws Exception {
        for (int id = 0; id < 5; id++) {
            try {
                if (lastNodeCount != nodeCount || lastMemory != memory
                        || !cpu.equals(lastCpu) || lastNetwork != network) {
                    if (VirtualBoxExperiments.monitor != null)
                        VirtualBoxExperiments.monitor.close();
                    VirtualBox.remove();
                    VirtualBox.setup(nodeCount, memory, (int) (Double
                            .parseDouble(cpu) * 100), network);
                    Thread.sleep(2000 * nodeCount);
                }
                String[] nodes = new String[nodeCount];
                if (VirtualBoxExperiments.monitor != null) {
                    VirtualBoxExperiments.monitor.close();
                    Thread.sleep(500);
                }
                VirtualBoxExperiments.monitor = new ClusterMonitor();
                VirtualBoxExperiments.monitor
                        .waitIp(nodes.length, maxStartTime);
                for (int i = 0; i < nodes.length; i++) {
                    nodes[i] = VirtualBoxExperiments.monitor.ip[i];
                    System.out.println("NC" + i + ": " + nodes[i]);
                }
                lastNodeCount = nodeCount;
                lastMemory = memory;
                lastCpu = cpu;
                lastNetwork = network;
                return nodes;
            } catch (ImruExperimentTimeoutException e) {
                lastNodeCount = 0;
                e.printStackTrace();
            } catch (Exception e) {
                throw e;
            }
        }
        throw new Error("Failed to start nodes after 5 times");
    }

    public static void stopNodes() throws Exception {
        lastNodeCount = 0;
        VirtualBox.remove();
        if (VirtualBoxExperiments.monitor != null)
            VirtualBoxExperiments.monitor.close();
    }

    public static void runExperiment(ImruExpParameters p) throws Exception {
        if (p.numOfDimensions < 0)
            throw new Error();
        File home = new File(System.getProperty("user.home"));
        String userName = "ubuntu";
        String[] nodes = new String[p.nodeCount];
        {
            Arrays.fill(nodes, "");
            LocalCluster cluster = new LocalCluster(new HyracksCluster("",
                    nodes, userName, new File(home, ".ssh/id_rsa")), userName);
            p.aggType = p.aggArg > 1 ? "nary" : "none";
            VirtualBoxExperiments exp = new VirtualBoxExperiments(cluster, p);
            if (exp.hasAllResults() || IMRU_ONLY && exp.hasResult(true)) {
                Rt.p("skip " + exp.resultDir.getPath());
                return;
            } else {
                Rt.p("run " + exp.resultDir.getPath());
            }
        }

        nodes = startNodes(p.nodeCount, p.memory, p.cpu, p.network,
                p.maxNodesStartupTime);

        HyracksNode.HYRACKS_PATH = "/home/" + userName + "/hyracks-ec2";
        String cc = nodes[0];
        LocalCluster cluster = new LocalCluster(new HyracksCluster(cc, nodes,
                userName, new File(home, ".ssh/id_rsa")), userName);
        //                File hyracksEc2Root = new File(home, "ucscImru/dist");
        //        cluster.cluster.install(hyracksEc2Root);
        p.aggType = p.aggArg > 1 ? "nary" : "none";
        VirtualBoxExperiments exp = new VirtualBoxExperiments(cluster, p);
        //      IMRUDebugger.debug = true;
        //                ImruDebugMonitor monitor = new ImruDebugMonitor(outputFile
        //                        .getAbsolutePath());
        if (IMRU_ONLY)
            exp.runIMRUMem();
        else
            exp.runExperiments();
        //                monitor.close();
        //        generateResult(exp.resultDir);
        //        stopNodes();
    }

    public static void createTemplate(String ip, String userName)
            throws Exception {
        File home = new File(System.getProperty("user.home"));
        String[] nodes = new String[] { ip };
        String cc = nodes[0];
        LocalCluster cluster = new LocalCluster(new HyracksCluster(cc, nodes,
                userName, new File(home, ".ssh/id_rsa")), userName);
        uploadExperimentCode(cluster, true);
    }

    static void testStratosphere() throws Exception {
        int nodeCount = 3;
        int memory = 2000;
        String cpu = "1";
        int network = 0;
        int numOfDimensions = 1000000;

        //        String[] nodes = startNodes(nodeCount, memory, cpu, network);
        String[] nodes = { "192.168.56.102", "192.168.56.103",
                "192.168.56.107", };
        VirtualBoxExperiments.monitor = new ClusterMonitor();
        File home = new File(System.getProperty("user.home"));

        String userName = "ubuntu";
        HyracksNode.HYRACKS_PATH = "/home/" + userName + "/hyracks-ec2";
        String cc = nodes[0];
        LocalCluster cluster = new LocalCluster(new HyracksCluster(cc, nodes,
                userName, new File(home, ".ssh/id_rsa")), userName);
        ImruExpParameters p = new ImruExpParameters();
        p.k = 1;
        p.iterations = 5;
        p.batchStart = 1;
        p.batchStep = 1;
        p.batchEnd = 1;
        p.batchSize = 100000;
        p.aggArg = 2;
        p.aggType = p.aggArg > 1 ? "nary" : "none";
        VirtualBoxExperiments exp = new VirtualBoxExperiments(cluster, p);
        exp.uploadExperimentCode(cluster, false);
        exp.runStratosphere();
        //        stopNodes();
    }

    public static void main(String[] args) throws Exception {
        //        testStratosphere();
        VirtualBox.remove();
        System.exit(0);
        //        createTemplate("192.168.56.110", "ubuntu");
        //        FanInAndK.runExp();
        //        DataPointsPerNode.runExp();
    }
}
