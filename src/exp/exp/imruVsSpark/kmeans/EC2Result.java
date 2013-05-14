package exp.imruVsSpark.kmeans;

import java.io.File;
import java.io.IOException;

import edu.uci.ics.hyracks.imru.util.Rt;
import exp.imruVsSpark.data.DataGenerator;
import exp.test0.GnuPlot;

public class EC2Result {
    public static void main(String[] args) throws Exception {
        GnuPlot plot = new GnuPlot(new File("/tmp/cache"), "kmeans",
                "Data points per node (10^5)", "Time (seconds)");
        plot.extra = "set title \"K=" + DataGenerator.DEBUG_K + ",Iteration="
                + DataGenerator.DEBUG_ITERATIONS + "\"";
        plot.setPlotNames("Generate Data", "Spark", "IMRU-mem", "IMRU-disk");
        plot.startPointType = 1;
        plot.pointSize = 1;
        int nodeCount = 2;
        File resultDir = new File("result/ec2_" + nodeCount + "nodes");
        String[] data = Rt.readFile(new File(resultDir, "generateTime.txt"))
                .split("\n");
        String[] imru = Rt.readFile(new File(resultDir, "imru.txt"))
                .split("\n");
        String[] spark = Rt.readFile(new File(resultDir, "spark.txt")).split(
                "\n");
        int i = 0;
        for (int aaa = EC2Benchmark.STARTC; aaa <= EC2Benchmark.ENDC; aaa += EC2Benchmark.STEPC, i++) {
            DataGenerator.DEBUG_DATA_POINTS = aaa * EC2Benchmark.BATCH;
            String[] d = data[i].split("\t");
            String[] m = imru[i].split("\t");
            String[] s = spark[i].split("\t");
            if ((int) Double.parseDouble(d[0]) != aaa)
                throw new Error();
            if ((int) Double.parseDouble(m[0]) != aaa)
                throw new Error();
            if ((int) Double.parseDouble(s[0]) != aaa)
                throw new Error();
            plot.startNewX(aaa);
            Rt.p(d[1] + "\t" + s[1] + "\t" + m[1] + "\t" + m[2]);
            plot.addY(Double.parseDouble(d[1]) / 1000);
            plot.addY(Double.parseDouble(s[1]));
            plot.addY(Double.parseDouble(m[1]));
            plot.addY(Double.parseDouble(m[2]));
            Rt.p(s[2] + "\t" + m[3] + "\t" + m[4]);
            if (!s[2].equals(m[3]))
                throw new Error();
            if (!s[2].equals(m[4]))
                throw new Error();
        }
        //        for (int i = 0; i < plot.vs.size(); i++)
        //            plot.vs.get(i).set(0, plot.vs.get(i).get(0) / 100000);
        plot.finish();
        Rt.runAndShowCommand("epstopdf --outfile="
                + new File(resultDir, nodeCount + "_nodes.pdf")
                        .getAbsolutePath() + " "
                + new File("/tmp/cache/kmeans.eps").getAbsolutePath());
        //        plot.show();
        System.exit(0);
    }
}
