package exp.imruVsSpark.kmeans;

import java.io.File;

import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.imru.example.utils.Client;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.imruVsSpark.data.DataGenerator;
import exp.imruVsSpark.kmeans.imru.IMRUKMeans;
import exp.imruVsSpark.kmeans.spark.SparkKMeans;
import exp.test0.GnuPlot;

public class EC2Benchmark {
    public static void exp(String master, int nodeCount, boolean imru)
            throws Exception {
        //        Client.disableLogging();
        DataGenerator.TEMPLATE = "/home/ubuntu/test/exp_data/product_name";
        new File("result").mkdir();
        GnuPlot plot = new GnuPlot(new File("result"), "kmeans"
                + (imru ? "imru" : "spark"), "Data points (10^5)",
                "Time (seconds)");
        plot.extra = "set title \"K=" + DataGenerator.DEBUG_K + ",Iteration="
                + DataGenerator.DEBUG_ITERATIONS + "\"";
        if (imru)
            plot.setPlotNames("Generate Data", "IMRU-mem", "IMRU-disk");
        else
            plot.setPlotNames("Generate Data", "Spark");
        plot.startPointType = 1;
        plot.pointSize = 1;
        //        plot.reloadData();
        //        for (int i = 0; i < plot.vs.size(); i++)
        //            plot.vs.get(i).set(0, plot.vs.get(i).get(0) / 100000);
        //        plot.finish();
        //        System.exit(0);

        int maxDataSize = 1000000;
        {
            File templateDir = new File(DataGenerator.TEMPLATE);
            final DataGenerator dataGenerator = new DataGenerator(maxDataSize
                    * nodeCount, templateDir);
            final int k = DataGenerator.DEBUG_K;
            final SKMeansModel model = new SKMeansModel(k, dataGenerator, 20);
            byte[] bs = JavaSerializationUtils.serialize(model);
            Rt.p("Max model size: %,d", bs.length);
        }
        for (DataGenerator.DEBUG_DATA_POINTS = 100000; DataGenerator.DEBUG_DATA_POINTS <= 1000000; DataGenerator.DEBUG_DATA_POINTS += 300000) {
            int dataSize = DataGenerator.DEBUG_DATA_POINTS * nodeCount;

            long start = System.currentTimeMillis();
            Rt.p("generating data");
            //            DataGenerator.main(new String[] { "/home/ubuntu/test/data.txt" });
            IMRUKMeans.generateData(master, DataGenerator.DEBUG_DATA_POINTS,
                    nodeCount);
            long dataTime = System.currentTimeMillis() - start;

            //            start = System.currentTimeMillis();
            //            SparseKMeans.run();
            //            long bareTime = System.currentTimeMillis() - start;
            if (imru) {
                Rt.p("running IMRU in memory");
                start = System.currentTimeMillis();
                IMRUKMeans.runEc2(master, nodeCount, dataSize, "/mnt/imru.txt",
                        true, false);
                long imruMemTime = System.currentTimeMillis() - start;

                //            start = System.currentTimeMillis();
                //            IMRUKMeans.runEc2(master, false, true);
                //            long imruParseTime = System.currentTimeMillis() - start;

                Rt.p("running IMRU in disk");
                start = System.currentTimeMillis();
                IMRUKMeans.runEc2(master, nodeCount, dataSize, "/mnt/imru.txt",
                        false, false);
                long imruDiskTime = System.currentTimeMillis() - start;
                plot.startNewX(DataGenerator.DEBUG_DATA_POINTS / 100000);
                plot.addY(dataTime / 1000.0);
                plot.addY(imruMemTime / 1000.0);
                plot.addY(imruDiskTime / 1000.0);
            } else {
                Rt.p("running spark");
                start = System.currentTimeMillis();
                SparkKMeans.run(master, dataSize, "/home/ubuntu/spark-0.7.0",
                        "/mnt/spark.txt", nodeCount);
                long sparkTime = System.currentTimeMillis() - start;
                plot.startNewX(DataGenerator.DEBUG_DATA_POINTS / 100000);
                plot.addY(dataTime / 1000.0);
                plot.addY(sparkTime / 1000.0);
            }
            plot.finish();
        }
        System.exit(0);
    }

    public static void main(String[] args) throws Exception {
        exp(args[0], Integer.parseInt(args[1]), Boolean.parseBoolean(args[2]));
    }
}
