package exp.imruVsSpark.kmeans.exp;

import java.io.File;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.ec2.HyracksCluster;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.imruVsSpark.LocalCluster;
import exp.imruVsSpark.VirtualBox;
import exp.imruVsSpark.kmeans.VirtualBoxExperiments;
import exp.imruVsSpark.kmeans.exp.dynamic.ImruSendOperator;
import exp.test0.GnuPlot;

public class Dynamic {
    public static void disableLogging() throws Exception {
        Logger globalLogger = Logger.getLogger("");
        Handler[] handlers = globalLogger.getHandlers();
        for (Handler handler : handlers)
            globalLogger.removeHandler(handler);
        globalLogger.addHandler(new Handler() {
            @Override
            public void publish(LogRecord record) {
                String s = record.getMessage();
                if (s.contains("Exception caught by thread")) {
                    System.err.println(s);
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() throws SecurityException {
            }
        });
    }

    public static void runExp() throws Exception {
        try {
            VirtualBox.remove();
            int nodeCount = 8;
            int memory = 2000;
            int k = 3;
            int iterations = 5;
            int batchStart = 1;
            int batchStep = 3;
            int batchEnd = 1;
            int batchSize = 100000;
            int network = 0;
            String cpu = "0.5";
            int fanIn = 2;

            VirtualBoxExperiments.IMRU_ONLY = true;
            for (int i = 0; i < 2; i++) {
                VirtualBoxExperiments.dynamicAggr = (i == 1);
                for (k = 3; k <= 3; k++) {
                    VirtualBoxExperiments.runExperiment(nodeCount, memory, k,
                            iterations, batchStart, batchStep, batchEnd,
                            batchSize, network, cpu, fanIn);
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(0);
        } finally {
        }
    }

    public static void runExp2() throws Exception {
        try {
            //            VirtualBox.remove();
            int nodeCount = 8;
            int memory = 2000;
            int k = 3;
            int iterations = 5;
            int batchStart = 1;
            int batchStep = 3;
            int batchEnd = 1;
            int batchSize = 100000;
            int network = 0;
            String cpu = "0.5";
            int fanIn = 2;
            String userName = "ubuntu";
            String name = "local" + memory + "M" + cpu + "coreN" + network;
            File home = new File(System.getProperty("user.home"));
            String[] nodes2 = ("NC0: 192.168.56.102\n"
                    + "NC1: 192.168.56.109\n" + "NC2: 192.168.56.104\n"
                    + "NC3: 192.168.56.103\n" + "NC4: 192.168.56.106\n"
                    + "NC5: 192.168.56.105\n" + "NC6: 192.168.56.108\n"
                    + "NC7: 192.168.56.107").split("\n");
            String[] nodes = new String[nodes2.length];
            for (int i = 0; i < nodes.length; i++)
                nodes[i] = nodes2[i].substring(4).trim();
            //            String[] nodes = VirtualBoxExperiments.startNodes(nodeCount,
            //                    memory, cpu, network);
            for (int i = 0; i < 2; i++) {
                ImruSendOperator.fixedTree = (i == 1);
                for (k = 3; k <= 3; k++) {
                    LocalCluster cluster = new LocalCluster(new HyracksCluster(
                            nodes[0], nodes, userName, new File(home,
                                    ".ssh/id_rsa")), userName);
//                    cluster.stopAll();
//                    Rt.p("testing IMRU");
//                    cluster.cluster.startHyrackCluster();
//                    Thread.sleep(5000);
//                    cluster.checkHyracks();
                    DynamicJob.main(new String[] { nodes[0] });
                    System.exit(0);
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(0);
        } finally {
        }
    }

    public static GnuPlot plot() throws Exception {
        GnuPlot plot = new GnuPlot(new File("/tmp/cache"), "kmeans100kK", "k",
                "Time (seconds)");
        File file = new File(KmeansFigs.figsDir, "k8i" + KmeansFigs.ITERATIONS
                + "b1s3e1b100000/local2000M0.5coreN0_8nodes_nary_2");
        KmeansFigs f = new KmeansFigs(file);
        plot.extra = "set title \"K-means" + " 10^5 points/node*" + f.nodeCount
                + " Iteration=" + f.iterations + "\\n cpu=" + f.core
                + "core/node*" + f.nodeCount + " memory=" + f.memory
                + "MB/node*" + f.nodeCount + " \"";
        plot.setPlotNames("Spark", "IMRU-disk", "IMRU-mem");
        plot.startPointType = 1;
        plot.pointSize = 1;
        plot.scale = false;
        plot.colored = true;
        for (int k = 2; k <= 8; k++) {
            f = new KmeansFigs(new File(KmeansFigs.figsDir, "k" + k + "i"
                    + KmeansFigs.ITERATIONS
                    + "b1s3e1b100000/local2000M0.5coreN0_8nodes_nary_2"));
            plot.startNewX(k);
            plot.addY(f.get("spark1"));
            plot.addY(f.get("imruDisk1"));
            plot.addY(f.get("imruMem1"));
        }
        plot.finish();
        return plot;
    }

    public static void main(String[] args) throws Exception {
        runExp2();
        //        plot().show();
    }
}
