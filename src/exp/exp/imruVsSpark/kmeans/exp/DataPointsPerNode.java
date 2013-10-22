package exp.imruVsSpark.kmeans.exp;

import java.io.File;

import exp.imruVsSpark.VirtualBox;
import exp.imruVsSpark.kmeans.VirtualBoxExperiments;
import exp.test0.GnuPlot;

public class DataPointsPerNode {
    public static void runExp() throws Exception {
        try {
            VirtualBox.remove();
            int nodeCount = 8;
            int memory = 2000;
            int k = 3;
            int iterations = 1;
            int batchStart = 1;
            int batchStep = 1;
            int batchEnd = 8;
            int batchSize = 100000;
            int network = 0;
            String cpu = "0.5";
            int fanIn = 2;

            VirtualBoxExperiments.runExperiment(nodeCount, memory, k,
                    iterations, batchStart, batchStep, batchEnd, batchSize,
                    network, cpu, fanIn);
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            System.exit(0);
        }
    }

    public static GnuPlot plot() throws Exception {
        GnuPlot plot = new GnuPlot(new File("/tmp/cache"), "kmeansSplitSize",
                "10^5 points/node", "Time (seconds)");
        File file = new File(
                "result/k3i1b1s1e8b100000/local2000M0.5coreN0_8nodes_nary_2");
        KmeansFigs f = new KmeansFigs(file);
        plot.extra = "set title \"K-means"
                //+ " 10^5 points/node*" + f.nodeCount
                + " Iteration=" + f.iterations + "\\n cpu=" + f.core
                + "core/node*" + f.nodeCount + " memory=" + f.memory
                + "MB/node*" + f.nodeCount + " \"";
        plot.setPlotNames("Spark", "IMRU-disk", "IMRU-mem");
        plot.startPointType = 1;
        plot.pointSize = 1;
        plot.scale = false;
        plot.colored = true;
        for (int batchSize = f.begin; batchSize < f.end; batchSize += f.step) {
            plot.startNewX(batchSize);
            plot.addY(f.get("spark" + batchSize));
            plot.addY(f.get("imruDisk" + batchSize));
            plot.addY(f.get("imruMem" + batchSize));
        }
        plot.finish();
        return plot;
    }

    public static void main(String[] args) throws Exception {
        //        runExp();
        plot().show();
    }
}
