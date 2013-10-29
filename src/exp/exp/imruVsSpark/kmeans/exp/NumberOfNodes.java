package exp.imruVsSpark.kmeans.exp;

import java.io.File;

import exp.imruVsSpark.VirtualBox;
import exp.imruVsSpark.kmeans.VirtualBoxExperiments;
import exp.test0.GnuPlot;

public class NumberOfNodes {
    public static void runExp() throws Exception {
        try {
            VirtualBox.remove();
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

            for (nodeCount = 1; nodeCount <= 16; nodeCount++)
                VirtualBoxExperiments.runExperiment(nodeCount, memory, k,
                        iterations, batchStart, batchStep, batchEnd, batchSize,
                        network, cpu, fanIn);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(0);
        } finally {
        }
    }

    public static GnuPlot plot() throws Exception {
        GnuPlot plot = new GnuPlot(new File("/tmp/cache"), "kmeans100Knodes",
                "Nodes", "Time (seconds)");
        File file = new File(KmeansFigs.figsDir, "k3i" + KmeansFigs.ITERATIONS
                + "b1s3e10b100000/local1500M0.25core_8nodes");
        file = new File(KmeansFigs.figsDir, "k3i" + KmeansFigs.ITERATIONS
                + "b1s3e1b100000/local1500M0.25coreN0_8nodes_nary_2");
        KmeansFigs f = new KmeansFigs(file);
        plot.extra = "set title \"K-means" + " 10^5 points/node K=" + f.k
                + " Iteration=" + f.iterations + "\\n cpu=" + f.core
                + "core/node" + " memory=" + f.memory + "MB/node \"";
        plot.setPlotNames("Spark", "IMRU-disk", "IMRU-mem");
        plot.startPointType = 1;
        plot.pointSize = 1;
        plot.scale = false;
        plot.colored = true;
        for (int nodeCount = 1; nodeCount <= 16; nodeCount++) {
            f = new KmeansFigs(new File(KmeansFigs.figsDir, "k3i"
                    + KmeansFigs.ITERATIONS
                    + "b1s3e1b100000/local1500M0.25coreN0_" + nodeCount
                    + "nodes_nary_2"));
            plot.startNewX(nodeCount);
            plot.addY(f.get("spark1"));
            plot.addY(f.get("imruDisk1"));
            plot.addY(f.get("imruMem1"));
        }
        plot.finish();
        return plot;
    }

    public static void main(String[] args) throws Exception {
        //        runExp();
        plot().show();
    }
}
