package exp.imruVsSpark.kmeans;

import java.io.File;
import java.io.IOException;
import java.util.Hashtable;

import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.imruVsSpark.data.DataGenerator;
import exp.test0.GnuPlot;

public class KmeansFigs extends Hashtable<String, Double> {
    public String name;
    public int memory;
    public String core;
    public int nodeCount;
    public int k, iterations, begin, step, end, batch;

    public KmeansFigs(File resultDir) throws IOException {
        name = resultDir.getName();
        memory = Integer.parseInt(name.substring(5, name.indexOf("M")));
        core = name.substring(name.indexOf("M") + 1, name.indexOf("core"));
        nodeCount = Integer.parseInt(name.substring(name.lastIndexOf("_", name
                .lastIndexOf("nodes")) + 1, name.lastIndexOf("nodes")));
        {
            String s = resultDir.getParentFile().getName();
            String[] ss = s.split("[kibse]");
            int pos = 1;
            k = Integer.parseInt(ss[pos++]);
            iterations = Integer.parseInt(ss[pos++]);
            begin = Integer.parseInt(ss[pos++]);
            step = Integer.parseInt(ss[pos++]);
            end = Integer.parseInt(ss[pos++]);
            batch = Integer.parseInt(ss[pos++]);
        }
        //        String[] data = Rt.readFile(new File(resultDir, "generateTime.txt"))
        //                .split("\n");
        String[] imruDisk = Rt.readFile(new File(resultDir, "imruDisk.txt"))
                .split("\n");
        String[] imruMem = Rt.readFile(new File(resultDir, "imruMem.txt"))
                .split("\n");
        File sparkFile = new File(resultDir, "spark.txt");
        String[] spark = !sparkFile.exists() ? null : Rt.readFile(sparkFile)
                .split("\n");
        String[] dataSizes = new String[imruDisk.length];
        String[] processed = new String[imruDisk.length];
        for (int i = 0; i < imruDisk.length; i++) {
            String[] ss1 = imruDisk[i].split("\t");
            double dataSize = Double.parseDouble(ss1[0]);
            dataSizes[i] = ss1[0];
            int dataSizeInt = (int) dataSize;

            String[] ss4 = imruDisk[i].split("\t");
            if (!ss4[0].equals(dataSizes[i]))
                throw new Error();
            double imruDiskTime = Double.parseDouble(ss4[1]);
            this.put("imruDisk" + dataSizeInt, imruDiskTime);
            processed[i] = ss4[2];

            if (spark != null) {
                String[] ss2 = spark[i].split("\t");
                if (!ss2[0].equals(dataSizes[i]))
                    throw new Error(spark[i] + " " + imruDisk[i]);
                double sparkTime = Double.parseDouble(ss2[1]);
                this.put("spark" + dataSizeInt, sparkTime);
                if (!ss2[2].equals(processed[i]))
                    throw new Error();
            }

            String[] ss3 = imruMem[i].split("\t");
            if (!ss3[0].equals(dataSizes[i]))
                throw new Error();
            double imruMemTime = Double.parseDouble(ss3[1]);
            this.put("imruMem" + dataSizeInt, imruMemTime);
            if (!ss3[2].equals(processed[i]))
                throw new Error(resultDir.getName() + " " + ss3[2] + " "
                        + processed[i]);
        }
    }

    public static void mem() throws Exception {
        GnuPlot plot = new GnuPlot(new File("/tmp/cache"), "kmeans1m",
                "Memory per node (MB)", "Time (seconds)");
        KmeansFigs f = new KmeansFigs(new File(
                "result/k3i5b1s3e10b100000/local1500M0.5core_8nodes"));
        plot.extra = "set title \"K-means" + " 10^6 *" + f.nodeCount
                + " data points\\n K=" + f.k + " Iteration=" + f.iterations
                + " cpu=" + f.core + "core/node*" + f.nodeCount + "node \"";
        plot.setPlotNames("Spark", "IMRU-disk", "IMRU-mem");
        plot.startPointType = 1;
        plot.pointSize = 1;
        plot.scale = false;
        plot.colored = true;
        for (int mem = 1500; mem <= 3000; mem += 500) {
            f = new KmeansFigs(new File("result/k3i5b1s3e10b100000/local" + mem
                    + "M0.5core_8nodes"));
            plot.startNewX(mem);
            plot.addY(f.get("spark10"));
            plot.addY(f.get("imruDisk10"));
            plot.addY(f.get("imruMem10"));
        }
        plot.finish();
        plot.show();
    }

    public static void nodes() throws Exception {
        GnuPlot plot = new GnuPlot(new File("/tmp/cache"), "kmeans1mnodes",
                "Nodes", "Time (seconds)");
        KmeansFigs f = new KmeansFigs(new File(
                "result/k3i5b1s3e10b100000/local1500M0.25core_8nodes"));
        plot.extra = "set title \"K-means" + " 10^5 points/node K=" + f.k
                + " Iteration=" + f.iterations + "\\n cpu=" + f.core
                + "core/node" + " memory=" + f.memory + "MB/node \"";
        plot.setPlotNames("Spark", "IMRU-disk", "IMRU-mem");
        plot.startPointType = 1;
        plot.pointSize = 1;
        plot.scale = false;
        plot.colored = true;
        for (int nodeCount = 1; nodeCount <= 16; nodeCount *= 2) {
            f = new KmeansFigs(new File(
                    "result/k3i5b1s3e10b100000/local1500M0.25core_" + nodeCount
                            + "nodes"));
            plot.startNewX(nodeCount);
            plot.addY(f.get("spark10"));
            plot.addY(f.get("imruDisk10"));
            plot.addY(f.get("imruMem10"));
        }
        plot.finish();
        plot.show();
    }

    public static void k() throws Exception {
        GnuPlot plot = new GnuPlot(new File("/tmp/cache"), "kmeans100kK",
                "Nodes", "Time (seconds)");
        KmeansFigs f = new KmeansFigs(new File(
                "result/k2i5b1s3e1b100000/local1500M0.25core_16nodes"));
        plot.extra = "set title \"K-means" + " 10^5 points/node"
                + " Iteration=" + f.iterations + "\\n cpu=" + f.core
                + "core/node" + " memory=" + f.memory + "MB/node \"";
        plot.setPlotNames("Spark", "IMRU-disk", "IMRU-mem");
        plot.startPointType = 1;
        plot.pointSize = 1;
        plot.scale = false;
        plot.colored = true;
        for (int k = 1; k <= 4; k *= 2) {
            f = new KmeansFigs(new File("result/k" + k
                    + "i5b1s3e1b100000/local1500M0.25core_16nodes"));
            plot.startNewX(k);
            plot.addY(f.get("spark1"));
            plot.addY(f.get("imruDisk1"));
            plot.addY(f.get("imruMem1"));
        }
        plot.finish();
        plot.show();
    }

    public static void network() throws Exception {
        GnuPlot plot = new GnuPlot(new File("/tmp/cache"), "kmeansNetwork",
                "Network Speed (MB/s)", "Time (seconds)");
        KmeansFigs f = new KmeansFigs(new File(
                "result/k3i5b1s3e10b100000/local2000M0.5coreN1_8nodes"));
        plot.extra = "set title \"K-means" + " 10^6 points/node"
                + " Iteration=" + f.iterations + "\\n cpu=" + f.core
                + "core/node*" + f.nodeCount + " memory=" + f.memory
                + "MB/node*" + f.nodeCount + " \"";
        plot.setPlotNames("Spark", "IMRU-disk", "IMRU-mem");
        plot.startPointType = 1;
        plot.pointSize = 1;
        plot.scale = false;
        plot.colored = true;
        int[] networkSpeed = { 1, 2, 5, 10, 100 };
        for (int network : networkSpeed) {
            f = new KmeansFigs(new File(
                    "result/k3i5b1s3e10b100000/local2000M0.5coreN" + network
                            + "_8nodes"));
            plot.startNewX(network);
            plot.addY(f.get("spark10"));
            plot.addY(f.get("imruDisk10"));
            plot.addY(f.get("imruMem10"));
        }
        plot.finish();
        plot.show();
    }

    public static void kAndFanOut() throws Exception {
        GnuPlot plotDisk = new GnuPlot(new File("/tmp/cache"), "kFanDisk", "k",
                "Time (seconds)");
        GnuPlot plotMem = new GnuPlot(new File("/tmp/cache"), "kFanMem", "k",
                "Time (seconds)");
        String name = "local2000M0.5coreN0_8nodes_";
        name="local1500M0.25coreN0_12nodes_";
        KmeansFigs f = new KmeansFigs(new File("result/k1i5b1s3e1b100000/"
                + name + "nary_2"));
        plotMem.extra = "set title \"K-means" + " 10^6 points/node"
                + " Iteration=" + f.iterations + "\\n cpu=" + f.core
                + "core/node*" + f.nodeCount + " memory=" + f.memory
                + "MB/node*" + f.nodeCount + " \"";
        GnuPlot[] ps = { plotMem, plotDisk };
        for (GnuPlot p : ps) {
            p.setPlotNames("no aggregation", "nary=2", "nary=3", "nary=4",
                    "nary=5");
            p.startPointType = 1;
            p.pointSize = 1;
            p.scale = false;
            p.colored = true;
            p.keyPosition = "bottom right";
        }
        for (int k = 1; k < 5; k++) {
            plotMem.startNewX(k);
            f = new KmeansFigs(new File("result/k" + k + "i5b1s3e1b100000/"
                    + name + "none_1"));
            plotDisk.addY(f.get("imruDisk1"));
            plotMem.addY(f.get("imruMem1"));
            for (int nary = 2; nary <= 5; nary++) {
                try {
                    f = new KmeansFigs(
                            new File(
                                    "result/k"
                                            + k
                                            + "i5b1s3e1b100000/"+name+"nary_"
                                            + nary));
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
        plotMem.show();
    }

    public static void main(String[] args) throws Exception {
        //TODO network framesize
        //TODO spark storage level
        //TODO fanout vs framesize
        //        mem();
        //        nodes();
        //        k();
        //        network();
        kAndFanOut();
        //1,210,439 -> 1,280,400
        //       14,525,646-> 15,365,178
        //                File templateDir = new File(DataGenerator.TEMPLATE);
        //                final DataGenerator dataGenerator = new DataGenerator(16000000,
        //                        templateDir);
        //                final int k = DataGenerator.DEBUG_K;
        //                final SKMeansModel model = new SKMeansModel(k, dataGenerator, 20);
        //                byte[] bs = JavaSerializationUtils.serialize(model);
        //                Rt.p("%,d",dataGenerator.dims);
        //                Rt.p("Max model size: %,d", bs.length);
    }
}
