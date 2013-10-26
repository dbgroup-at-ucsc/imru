package exp.imruVsSpark.kmeans.exp;

import java.io.File;

import exp.imruVsSpark.VirtualBox;
import exp.imruVsSpark.kmeans.VirtualBoxExperiments;
import exp.test0.GnuPlot;

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

            iterations = 5;

            for (fanIn = 2; fanIn <= 7; fanIn++) {
                if (fanIn == 1)
                    continue;
                for (k = 1; k <= 5; k++) {
                    //                File outputFile = new File("result/fan16_" + fanIn + "_" + k
                    //                        + ".txt");
                    //                if (outputFile.exists() && outputFile.length() > 0)
                    //                    continue;
                    VirtualBoxExperiments.IMRU_ONLY = true;
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

    public static GnuPlot plot() throws Exception {
        GnuPlot plotDisk = new GnuPlot(new File("/tmp/cache"), "kFanDisk8n1",
                "fan-in", "Time (seconds)");
        GnuPlot plotMem = new GnuPlot(new File("/tmp/cache"), "kFanMem8n1",
                "fan-in", "Time (seconds)");
        String name = "local2000M0.5coreN0_8nodes_";
        name = "local1500M0.25coreN0_12nodes_";
        name = "local1500M0.25coreN0_16nodes_";
        File file = new File("result/k2i5b1s3e1b100000/" + name + "nary_2");
        KmeansFigs f = new KmeansFigs(file);
        plotMem.extra = "set title \"K-means" + " 10^6 points/node"
                + " Iteration=" + f.iterations + "\\n cpu=" + f.core
                + "core/node*" + f.nodeCount + " memory=" + f.memory
                + "MB/node*" + f.nodeCount + " \"";
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
                    f = new KmeansFigs(new File("result/k" + k
                            + "i5b1s3e1b100000/" + name + "nary_" + nary));
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
        plotDisk.finish();
        return plotMem;
    }

    public static void main(String[] args) throws Exception {
//                runExp();
        plot().show();
    }
}
