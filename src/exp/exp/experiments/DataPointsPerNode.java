package exp.experiments;

import java.io.File;

import exp.ImruExpFigs;
import exp.VirtualBoxExperiments;
import exp.imruVsSpark.VirtualBox;
import exp.test0.GnuPlot;
import exp.types.ImruExpParameters;

public class DataPointsPerNode {
    public static void runExp() throws Exception {
        try {
            VirtualBox.remove();
            ImruExpParameters p = new ImruExpParameters();
            p.nodeCount = 8;
            p.memory = 2000;
            p.k = 3;
            p.iterations = 5;
            p.batchStart = 1;
            p.batchStep = 1;
            p.batchEnd = 8;
            p.batchSize = 100000;
            p.numOfDimensions = 1000000;
            p.network = 0;
            p.cpu = "0.5";
            p.aggArg = 2;

            VirtualBoxExperiments.runExperiment(p);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(0);
        } finally {
        }
    }

    public static GnuPlot plot() throws Exception {
        GnuPlot plot = new GnuPlot(new File("/tmp/cache"), "kmeansSplitSize",
                "10^5 points/node", "Time (seconds)");
        File file = new File(ImruExpFigs.figsDir, "k3i"
                + ImruExpFigs.ITERATIONS
                + "b1s1e8b100000/local2000M0.5coreN0_8nodes_nary_2");
        ImruExpFigs f = new ImruExpFigs(file);
        plot.extra = "set title \"K-means"
                //+ " 10^5 points/node*" + f.nodeCount
                + " Iteration=" + f.p.iterations + "\\n cpu=" + f.p.cpu
                + "core/node*" + f.p.nodeCount + " memory=" + f.p.memory
                + "MB/node*" + f.p.nodeCount + " \"";
        plot.setPlotNames("Spark", "IMRU-disk", "IMRU-mem");
        plot.startPointType = 1;
        plot.pointSize = 1;
        plot.scale = false;
        plot.colored = true;
        for (int batchSize = f.p.batchStart; batchSize <= f.p.batchEnd; batchSize += f.p.batchStep) {
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
