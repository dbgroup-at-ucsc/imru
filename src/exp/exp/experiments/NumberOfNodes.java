package exp.experiments;

import java.io.File;

import exp.ImruExpFigs;
import exp.VirtualBoxExperiments;
import exp.imruVsSpark.VirtualBox;
import exp.test0.GnuPlot;
import exp.types.ImruExpParameters;

public class NumberOfNodes {
    public static void runExp() throws Exception {
        try {
            VirtualBox.remove();
            ImruExpParameters p = new ImruExpParameters();
            p.nodeCount = 16;
            p.memory = 1500;
            p.k = 3;
            p.iterations = 5;
            p.batchStart = 1;
            p.batchStep = 3;
            p.batchEnd = 1;
            p.batchSize = 100000;
            p.network = 0;
            p.cpu = "0.25";
            p.aggArg = 2;

            for (p.nodeCount = 1; p.nodeCount <= 16; p.nodeCount++)
                VirtualBoxExperiments.runExperiment(p);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(0);
        } finally {
        }
    }

    public static GnuPlot plot() throws Exception {
        GnuPlot plot = new GnuPlot(new File("/tmp/cache"), "kmeans100Knodes",
                "Nodes", "Time (seconds)");
        File file = new File(ImruExpFigs.figsDir, "k3i" + ImruExpFigs.ITERATIONS
                + "b1s3e10b100000/local1500M0.25core_8nodes");
        file = new File(ImruExpFigs.figsDir, "k3i" + ImruExpFigs.ITERATIONS
                + "b1s3e1b100000/local1500M0.25coreN0_8nodes_nary_2");
        ImruExpFigs f = new ImruExpFigs(file);
        plot.extra = "set title \"K-means" + " 10^5 points/node K=" + f.p.k
                + " Iteration=" + f.p.iterations + "\\n cpu=" + f.p.cpu
                + "core/node" + " memory=" + f.p.memory + "MB/node \"";
        plot.setPlotNames("Spark", "IMRU-disk", "IMRU-mem");
        plot.startPointType = 1;
        plot.pointSize = 1;
        plot.scale = false;
        plot.colored = true;
        for (int nodeCount = 1; nodeCount <= 16; nodeCount++) {
            f = new ImruExpFigs(new File(ImruExpFigs.figsDir, "k3i"
                    + ImruExpFigs.ITERATIONS
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
