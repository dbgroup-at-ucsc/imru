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
        nodeCount = Integer.parseInt(name.substring(name.lastIndexOf("_") + 1,
                name.length() - 5));
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
        String[] spark = Rt.readFile(new File(resultDir, "spark.txt")).split(
                "\n");
        String[] dataSizes = new String[imruDisk.length];
        String[] processed = new String[imruDisk.length];
        for (int i = 0; i < imruDisk.length; i++) {
            String[] ss1 = imruDisk[i].split("\t");
            double dataSize = Double.parseDouble(ss1[0]);
            dataSizes[i] = ss1[0];
            int dataSizeInt = (int) dataSize;

            String[] ss2 = spark[i].split("\t");
            if (!ss2[0].equals(dataSizes[i]))
                throw new Error(spark[i] + " " + imruDisk[i]);
            double sparkTime = Double.parseDouble(ss2[1]);
            this.put("spark" + dataSizeInt, sparkTime);
            processed[i] = ss2[2];

            String[] ss4 = imruDisk[i].split("\t");
            if (!ss4[0].equals(dataSizes[i]))
                throw new Error();
            double imruDiskTime = Double.parseDouble(ss4[1]);
            this.put("imruDisk" + dataSizeInt, imruDiskTime);
            if (!ss4[2].equals(processed[i]))
                throw new Error();

            String[] ss3 = imruMem[i].split("\t");
            if (!ss3[0].equals(dataSizes[i]))
                throw new Error();
            double imruMemTime = Double.parseDouble(ss3[1]);
            this.put("imruMem" + dataSizeInt, imruMemTime);
            if (!ss3[2].equals(processed[i]))
                throw new Error();
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
        GnuPlot plot = new GnuPlot(new File("/tmp/cache"), "kmeans100knodes",
                "Nodes", "Time (seconds)");
        KmeansFigs f = new KmeansFigs(new File(
                "result/k3i5b1s3e10b100000/local1500M0.5core_8nodes"));
        plot.extra = "set title \"K-means" + " 10^5 points/node K=" + f.k
                + " Iteration=" + f.iterations + "\\n cpu=" + f.core + "core/node"
                + " memory=" + f.memory + "MB/node \"";
        plot.setPlotNames("Spark", "IMRU-disk", "IMRU-mem");
        plot.startPointType = 1;
        plot.pointSize = 1;
        plot.scale = false;
        plot.colored = true;
        for (int nodeCount = 1; nodeCount <= 16; nodeCount *= 2) {
            f = new KmeansFigs(new File(
                    "result/k3i5b1s3e1b100000/local1500M0.25core_" + nodeCount
                            + "nodes"));
            plot.startNewX(nodeCount);
            plot.addY(f.get("spark1"));
            plot.addY(f.get("imruDisk1"));
            plot.addY(f.get("imruMem1"));
        }
        plot.finish();
        plot.show();
    }

    public static void main(String[] args) throws Exception {
        //TODO network framesize
        //TODO spark storage level
        //        mem();
//         nodes();
        //       14,525,646-> 15,365,178
        //        File templateDir = new File(DataGenerator.TEMPLATE);
        //        final DataGenerator dataGenerator = new DataGenerator(100000,
        //                templateDir);
        //        final int k = DataGenerator.DEBUG_K;
        //        final SKMeansModel model = new SKMeansModel(k, dataGenerator, 20);
        //        byte[] bs = JavaSerializationUtils.serialize(model);
        //        Rt.p("Max model size: %,d", bs.length);

    }
}
