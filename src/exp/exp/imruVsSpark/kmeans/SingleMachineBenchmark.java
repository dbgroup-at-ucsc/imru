package exp.imruVsSpark.kmeans;

import java.io.File;

import edu.uci.ics.hyracks.ec2.Rt;
import edu.uci.ics.hyracks.imru.util.Client;
import exp.imruVsSpark.data.DataGenerator;
import exp.imruVsSpark.kmeans.imru.IMRUKMeans;
import exp.imruVsSpark.kmeans.spark.SparkKMeans;
import exp.test0.GnuPlot;
import exp.test0.lr.ImruLR;
import exp.test0.lr.LR;
import exp.test0.lr.SparkLR;

public class SingleMachineBenchmark {
    public static void main(String[] args) throws Exception {
        Client.disableLogging();
        int k = 3;
        int iterations = 5;
        GnuPlot plot = new GnuPlot(new File("result"), "kmeans",
                "Data points (10^5)", "Time (seconds)");
        plot.extra = "set title \"K=" + k + ",Iteration=" + iterations + "\"";
        plot.setPlotNames("Generate Data", "Bare", "Spark", "IMRU-mem",
                "IMRU-disk", "IMRU-parse");
        plot.startPointType = 1;
        plot.pointSize = 1;
        plot.reloadData();
        //        for (int i = 0; i < plot.vs.size(); i++)
        //            plot.vs.get(i).set(0, plot.vs.get(i).get(0) / 100000);
        plot.finish();
        System.exit(0);
        int numOfDimensions = 1000000;
        for (int points = 100000; points <= 1000000; points += 100000) {
            long start = System.currentTimeMillis();
            DataGenerator.main(args);
            long dataTime = System.currentTimeMillis() - start;

            start = System.currentTimeMillis();
            SparseKMeans.run("/data/b/data/imru/productName.txt");
            long bareTime = System.currentTimeMillis() - start;

            start = System.currentTimeMillis();
            IMRUKMeans.run(true, false, k, iterations, points, numOfDimensions,
                    0);
            long imruMemTime = System.currentTimeMillis() - start;

            start = System.currentTimeMillis();
            IMRUKMeans.run(false, true, k, iterations, points, numOfDimensions,
                    0);
            long imruParseTime = System.currentTimeMillis() - start;

            start = System.currentTimeMillis();
            IMRUKMeans.run(false, false, k, iterations, points,
                    numOfDimensions, 0);
            long imruDiskTime = System.currentTimeMillis() - start;

            start = System.currentTimeMillis();
            String host = "192.168.56.101";
            host = "10.243.74.41";
            SparkKMeans.run(host, points, numOfDimensions,"/data/b/soft/spark-0.7.0",
                    "/data/b/data/imru/productName.txt", 1, k, iterations);
            long sparkTime = System.currentTimeMillis() - start;

            Rt.p("Data: %,d", dataTime);
            Rt.p("Bare: %,d", bareTime);
            Rt.p("Spark: %,d", sparkTime);
            Rt.p("IMRU-mem: %,d", imruMemTime);
            Rt.p("IMRU-disk: %,d", imruDiskTime);
            Rt.p("IMRU-parse: %,d", imruParseTime);

            plot.startNewX(points / 100000);
            plot.addY(dataTime / 1000.0);
            plot.addY(bareTime / 1000.0);
            plot.addY(sparkTime / 1000.0);
            plot.addY(imruMemTime / 1000.0);
            plot.addY(imruDiskTime / 1000.0);
            plot.addY(imruParseTime / 1000.0);
            plot.finish();
        }
        System.exit(0);
    }
}
