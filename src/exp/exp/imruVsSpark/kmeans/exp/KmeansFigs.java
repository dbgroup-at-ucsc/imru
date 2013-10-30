package exp.imruVsSpark.kmeans.exp;

import java.io.File;
import java.io.IOException;
import java.util.Hashtable;

import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.imruVsSpark.data.DataGenerator;
import exp.test0.GnuPlot;

public class KmeansFigs extends Hashtable<String, Double> {
    public static File figsDir = new File("results");
    public static int ITERATIONS = 5;
    public String name;
    public int memory;
    public String core;
    public int nodeCount;
    public int k, iterations, begin, step, end, batch;

    public KmeansFigs(File resultDir) throws IOException {
        name = resultDir.getName();
        Rt.p("reading " + name);
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
        File imruDiskFile = new File(resultDir, "imruDisk.txt");
        File imruMemFile = new File(resultDir, "imruMem.txt");
        File sparkFile = new File(resultDir, "spark.txt");
        String[] imruDisk = null;
        String[] imruMem = null;
        String[] spark = null;
        if (imruDiskFile.exists())
            imruDisk = Rt.readFile(imruDiskFile).split("\n");
        else
            Rt.p("imru disk failed");
        if (imruMemFile.exists())
            imruMem = !imruMemFile.exists() ? null : Rt.readFile(imruMemFile)
                    .split("\n");
        else
            Rt.p("imru memory failed");
        if (sparkFile.exists())
            spark = !sparkFile.exists() ? null : Rt.readFile(sparkFile).split(
                    "\n");
        else
            Rt.p("spark failed");
        int n = 0;
        if (imruMem != null)
            n = imruMem.length;
        else if (imruDisk != null)
            n = imruDisk.length;
        else if (spark != null)
            n = spark.length;
        String[] dataSizes = new String[n];
        String[] processed = new String[n];
        for (int i = 0; i < n; i++) {
            String[] ss1 = imruMem[i].split("\t");
            double dataSize = Double.parseDouble(ss1[0]);
            dataSizes[i] = ss1[0];
            int dataSizeInt = (int) dataSize;
            processed[i] = ss1[2];

            if (imruDisk != null) {
                String[] ss4 = imruDisk[i].split("\t");
                if (!ss4[0].equals(dataSizes[i]))
                    throw new Error();
                double imruDiskTime = Double.parseDouble(ss4[1]);
                this.put("imruDisk" + dataSizeInt, imruDiskTime);
            }

            if (spark != null) {
                String[] ss2 = spark[i].split("\t");
                if (!ss2[0].equals(dataSizes[i]))
                    throw new Error(spark[i] + " " + imruDisk[i]);
                double sparkTime = Double.parseDouble(ss2[1]);
                this.put("spark" + dataSizeInt, sparkTime);
                if (!ss2[2].equals(processed[i]))
                    throw new Error(k + " " + name + " " + ss2[2] + " "
                            + processed[i]);
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

    public static void main(String[] args) throws Exception {
        //        figsDir = new File("result1");
        DataPointsPerNode.plot();
        ModelSize.plot();
        NumberOfNodes.plot();
        FanInAndK.plot();
        //TODO network framesize
        //TODO spark storage level
        //TODO fanout vs framesize
        //        mem();
        //        network();
        //                kAndFanOut();
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
