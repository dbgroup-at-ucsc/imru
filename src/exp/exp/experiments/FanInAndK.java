package exp.experiments;

import java.io.File;

import exp.ImruExpFigs;
import exp.VirtualBoxExperiments;
import exp.imruVsSpark.VirtualBox;
import exp.test0.GnuPlot;
import exp.types.ImruExpParameters;

public class FanInAndK {
    public static void runExp() throws Exception {
        //        generateResult(new File(
        //                "result/k3i1b1s3e10b100000/local1500M0.25core_16nodes"));
        //        generateResult(new File(
        //                "result/k3i1b1s3e10b100000/local1500M0.5core_8nodes"));
        //        System.exit(0);
        //        regenerateResults();
        try {
            //            System.exit(0);
            ImruExpParameters p = new ImruExpParameters();
            p.nodeCount = 16;
            p.memory = 1500;
            p.k = 3;
            p.iterations = 5;
            p.batchStart = 1;
            p.batchStep = 3;
            p.batchEnd = 1;
            p.batchSize = 100000;
            p.numOfDimensions = 1000000;
            p.network = 0;
            p.cpu = "0.25";
            p.aggArg = 2;

            p.iterations = 5;

            for (p.aggArg = 2; p.aggArg <= 7; p.aggArg++) {
                if (p.aggArg == 1)
                    continue;
                for (p.k = 1; p.k <= 5; p.k++) {
                    //                File outputFile = new File("result/fan16_" + fanIn + "_" + k
                    //                        + ".txt");
                    //                if (outputFile.exists() && outputFile.length() > 0)
                    //                    continue;
                    VirtualBoxExperiments.IMRU_ONLY = true;
                    VirtualBoxExperiments.runExperiment(p);
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(0);
        } finally {
        }
    }

    public static GnuPlot plot() throws Exception {
        GnuPlot plotDisk = new GnuPlot(new File("/tmp/cache"), "kFanDisk8n1",
                "fan-in", "Time (seconds)");
        GnuPlot plotMem = new GnuPlot(new File("/tmp/cache"), "kFanMem8n1",
                "fan-in", "Time (seconds)");
        String name = "local2000M0.5coreN0_8nodes_";
        name = "local1500M0.25coreN0_12nodes_";
        name = "local1500M0.25coreN0_16nodes_";
        File file = new File(ImruExpFigs.figsDir, "k2i" + ImruExpFigs.ITERATIONS
                + "b1s3e1b100000/" + name + "nary_2");
        ImruExpFigs f = new ImruExpFigs(file);
        plotMem.extra = "set title \"K-means" + " 10^6 points/node"
                + " Iteration=" + f.p.iterations + "\\n cpu=" + f.p.cpu
                + "core/node*" + f.p.nodeCount + " memory=" + f.p.memory
                + "MB/node*" + f.p.nodeCount + " \"";
        GnuPlot[] ps = { plotMem, plotDisk };
        for (GnuPlot p : ps) {
            p.setPlotNames("no aggregation", "nary=2", "nary=3", "nary=4",
                    "nary=5");
            p.setPlotNames("k=2", "k=3", "k=4", "k=5");
            p.startPointType = 1;
            p.pointSize = 1;
            p.scale = false;
            p.colored = true;
            p.keyPosition = "bottom right";
        }
        for (int nary = 2; nary <= 7; nary++) {
            plotMem.startNewX(nary);
            plotDisk.startNewX(nary);
            for (int k = 2; k < 6; k++) {
                try {
                    f = new ImruExpFigs(new File(ImruExpFigs.figsDir, "k" + k
                            + "i" + ImruExpFigs.ITERATIONS + "b1s3e1b100000/"
                            + name + "nary_" + nary));
                } catch (Throwable e) {
                    e.printStackTrace();
                    plotDisk.addY(0);
                    plotMem.addY(0);
                    continue;
                }
                plotDisk.addY(f.get("imruDisk1"));
                plotMem.addY(f.get("imruMem1"));
            }
        }
        plotMem.finish();
        //        plotDisk.finish();
        return plotMem;
    }

    public static void main(String[] args) throws Exception {
        //                runExp();
        plot().show();
    }
}
