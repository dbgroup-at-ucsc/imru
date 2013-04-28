package exp.imruVsSpark.kmeans;

import java.io.File;

import edu.uci.ics.hyracks.ec2.Rt;
import edu.uci.ics.hyracks.imru.example.utils.Client;
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
        GnuPlot plot = new GnuPlot(new File("result"), "kmeans", "Data points", "Time (seconds)");
        plot.extra = "set title \"K=" + DataGenerator.DEBUG_K + ",Iteration=" + DataGenerator.DEBUG_ITERATIONS + "\"";
        plot.setPlotNames("Spark", "IMRU");
        for (DataGenerator.DEBUG_DATA_POINTS = 100; DataGenerator.DEBUG_DATA_POINTS <= 10000; DataGenerator.DEBUG_DATA_POINTS *= 10) {
            DataGenerator.main(args);
            long start = System.currentTimeMillis();
            IMRUKMeans.run();
            long imruTime = System.currentTimeMillis() - start;
            start = System.currentTimeMillis();
            SparkKMeans.run();
            long sparkTime = System.currentTimeMillis() - start;
            Rt.np("Spark: " + sparkTime);
            Rt.np("IMRU: " + imruTime);
            plot.startNewX(DataGenerator.DEBUG_DATA_POINTS);
            plot.addY(sparkTime / 1000.0);
            plot.addY(imruTime / 1000.0);
        }
        plot.finish();
        System.exit(0);
    }
}
