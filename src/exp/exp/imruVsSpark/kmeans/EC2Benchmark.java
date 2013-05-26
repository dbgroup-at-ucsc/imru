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
    public static String dataPath = "/home/ubuntu/data";//"/data/b/data/imru";
    public static int STARTC = 1;
    public static int ENDC = 10;
    public static int BATCH = 100000;
    public static int STEPC = 3;

    public static void exp(String master, int nodeCount, String type)
            throws Exception {
        boolean mem = false;
        String user = "ubuntu";
        //        Client.disableLogging();
        DataGenerator.TEMPLATE = "/home/ubuntu/test/exp_data/product_name";
        if (!new File(DataGenerator.TEMPLATE).exists()) {
            user = "wangrui";
            DataGenerator.TEMPLATE = "/home/wangrui/test/exp_data/product_name";
        }
        new File("result").mkdir();
        GnuPlot plot = new GnuPlot(new File("result"), "kmeans" + type,
                "Data points (10^5)", "Time (seconds)");
        plot.extra = "set title \"K=" + DataGenerator.DEBUG_K + ",Iteration="
                + DataGenerator.DEBUG_ITERATIONS + "\"";
        plot.setPlotNames(type, "data");
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
        for (int aaa = EC2Benchmark.STARTC; aaa <= EC2Benchmark.ENDC; aaa += EC2Benchmark.STEPC) {
            DataGenerator.DEBUG_DATA_POINTS = aaa * EC2Benchmark.BATCH;
            int dataSize = DataGenerator.DEBUG_DATA_POINTS * nodeCount;

            long start = System.currentTimeMillis();
            //            Rt.p("generating data");
            //            DataGenerator.main(new String[] { "/home/ubuntu/test/data.txt" });
            //            IMRUKMeans.generateData(master, DataGenerator.DEBUG_DATA_POINTS,
            //                    nodeCount,new File(DataGenerator.TEMPLATE),);
            //            long dataTime = System.currentTimeMillis() - start;

            //            start = System.currentTimeMillis();
            //            SparseKMeans.run();
            //            long bareTime = System.currentTimeMillis() - start;
            if ("imru".equals(type)) {
                Rt.p("running IMRU in disk " + aaa);
                start = System.currentTimeMillis();
                int processed2 = IMRUKMeans.runEc2(master, nodeCount, dataSize,
                        dataPath + "/imru" + aaa + ".txt", false, false);
                long imruDiskTime = System.currentTimeMillis() - start;
                plot.startNewX(DataGenerator.DEBUG_DATA_POINTS / 100000);
                //                plot.addY(dataTime / 1000.0);
                plot.addY(imruDiskTime / 1000.0);
                plot.addY(processed2);
            } else if ("imruMem".equals(type)) {
                Rt.p("running IMRU in memory " + aaa);
                start = System.currentTimeMillis();
                int processed1 = 0;
                if (mem)
                    processed1 = IMRUKMeans.runEc2(master, nodeCount, dataSize,
                            dataPath + "/imru" + aaa + ".txt", true, false);
                long imruMemTime = System.currentTimeMillis() - start;

                //            start = System.currentTimeMillis();
                //            IMRUKMeans.runEc2(master, false, true);
                //            long imruParseTime = System.currentTimeMillis() - start;
                plot.startNewX(DataGenerator.DEBUG_DATA_POINTS / 100000);
                //                plot.addY(dataTime / 1000.0);
                plot.addY(imruMemTime / 1000.0);
                plot.addY(processed1);
            } else if ("spark".equals(type)) {
                Rt.p("running spark " + aaa);
                start = System.currentTimeMillis();
                int processed = SparkKMeans.run(master, dataSize, "/home/"
                        + user + "/spark-0.7.0", dataPath + "/spark" + aaa
                        + ".txt", nodeCount);
                long sparkTime = System.currentTimeMillis() - start;
                plot.startNewX(DataGenerator.DEBUG_DATA_POINTS / 100000);
                //                plot.addY(dataTime / 1000.0);
                plot.addY(sparkTime / 1000.0);
                plot.addY(processed);
            } else {
                throw new Error();
            }
            plot.finish();
        }
        System.exit(0);
    }

    public static void main(String[] args) throws Exception {
        exp(args[0], Integer.parseInt(args[1]), args[2]);
    }
}
