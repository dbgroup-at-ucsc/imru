package exp.imruVsSpark.kmeans;

import java.io.File;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.imru.example.utils.Client;
import edu.uci.ics.hyracks.imru.example.utils.Client.Options;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.imruVsSpark.data.DataGenerator;
import exp.imruVsSpark.kmeans.imru.IMRUKMeans;
import exp.imruVsSpark.kmeans.spark.SparkKMeans;
import exp.imruVsSpark.kmeans.stratosphere.StratosphereKMeans;
import exp.test0.GnuPlot;

public class KmeansExperiment {
    public static String getImruDataPath(int sizePerNode, int nodeCount,
            String nodeId) {
        return "/data/size" + sizePerNode + "/nodes" + nodeCount + "/imru"
                + nodeId + ".txt";
    }

    public static String getSparkDataPath(int sizePerNode, int nodeCount) {
        return "/data/size" + sizePerNode + "/nodes" + nodeCount + "/spark.txt";
    }

    public static void exp(String master, int nodeCount, String type, int k,
            int iterations, int startBatch, int stepBatch, int stopBatch,
            int batchSize, String aggType, int aggArg, boolean dynamic)
            throws Exception {
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
        plot.extra = "set title \"K=" + k + ",Iteration=" + iterations + "\"";
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
            final SKMeansModel model = new SKMeansModel(k, dataGenerator, 20);
            byte[] bs = JavaSerializationUtils.serialize(model);
            Rt.p("Max model size: %,d", bs.length);
        }
        for (int sizePerNode = startBatch; sizePerNode <= stopBatch; sizePerNode += stepBatch) {
            int pointPerNode = sizePerNode * batchSize;
            int dataSize = pointPerNode * nodeCount;

            long start = System.currentTimeMillis();
            //            Rt.p("generating data");
            //            DataGenerator.main(new String[] { "/home/ubuntu/test/data.txt" });
            //            IMRUKMeans.generateData(master, pointPerNode,
            //                    nodeCount,new File(DataGenerator.TEMPLATE),);
            //            long dataTime = System.currentTimeMillis() - start;

            //            start = System.currentTimeMillis();
            //            SparseKMeans.run();
            //            long bareTime = System.currentTimeMillis() - start;
            if ("imruDisk".equals(type)) {
                Rt.p("running IMRU in disk " + sizePerNode);
                start = System.currentTimeMillis();
                String path = getImruDataPath(sizePerNode, nodeCount, "%d");
                int processed2 = IMRUKMeans.runEc2(master, nodeCount, dataSize,
                        path, false, false, k, iterations, aggType, aggArg,
                        dynamic);
                long imruDiskTime = System.currentTimeMillis() - start;
                plot.startNewX(pointPerNode / 100000);
                //                plot.addY(dataTime / 1000.0);
                plot.addY(imruDiskTime / 1000.0);
                plot.addY(processed2);
            } else if ("imruMem".equals(type)) {
                Rt.p("running IMRU in memory " + sizePerNode);
                start = System.currentTimeMillis();
                String path = getImruDataPath(sizePerNode, nodeCount, "%d");
                int processed1 = IMRUKMeans.runEc2(master, nodeCount, dataSize,
                        path, true, false, k, iterations, aggType, aggArg,
                        dynamic);
                long imruMemTime = System.currentTimeMillis() - start;

                //            start = System.currentTimeMillis();
                //            IMRUKMeans.runEc2(master, false, true);
                //            long imruParseTime = System.currentTimeMillis() - start;
                plot.startNewX(pointPerNode / 100000);
                //                plot.addY(dataTime / 1000.0);
                plot.addY(imruMemTime / 1000.0);
                plot.addY(processed1);
            } else if ("spark".equals(type)) {
                Rt.p("running spark " + sizePerNode);
                start = System.currentTimeMillis();
                String path = getSparkDataPath(sizePerNode, nodeCount);
                int processed = SparkKMeans.run(master, dataSize, "/home/"
                        + user + "/spark-0.8.0-incubating", path, nodeCount, k,
                        iterations);
                long sparkTime = System.currentTimeMillis() - start;
                plot.startNewX(pointPerNode / 100000);
                //                plot.addY(dataTime / 1000.0);
                plot.addY(sparkTime / 1000.0);
                plot.addY(processed);
            } else if ("stratosphere".equals(type)) {
                Rt.p("running stratosphere " + sizePerNode);
                start = System.currentTimeMillis();
                String path = getSparkDataPath(sizePerNode, nodeCount);
                int processed = StratosphereKMeans.run(master, dataSize,
                        "/home/" + user + "/spark-0.8.0-incubating", path,
                        nodeCount, k, iterations);
                long sparkTime = System.currentTimeMillis() - start;
                plot.startNewX(pointPerNode / 100000);
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

    public static class Options {
        @Option(name = "-master", required = true)
        String master;
        @Option(name = "-nodeCount", required = true)
        int nodeCount;
        @Option(name = "-type", required = true)
        String type;
        @Option(name = "-k", required = true)
        int k;
        @Option(name = "-iterations", required = true)
        int iterations;
        @Option(name = "-batchStart", required = true)
        int batchStart;
        @Option(name = "-batchEnd", required = true)
        int batchEnd;
        @Option(name = "-batchStep", required = true)
        int batchStep;
        @Option(name = "-batchSize", required = true)
        int batchSize;
        @Option(name = "-agg-tree-type", usage = "The aggregation tree type (none, nary, or generic)")
        public String aggTreeType;
        @Option(name = "-agg-count", usage = "The number of aggregators to use, if using an aggregation tree")
        public int aggCount = -1;
        @Option(name = "-fan-in", usage = "The fan-in, if using an nary aggregation tree")
        public int fanIn = -1;
        @Option(name = "-dynamic")
        public boolean dynamic;
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            args = "-master 192.168.56.104 -nodeCount 2 -type imruMem -k 6 -iterations 1 -batchStart 1 -batchStep 3 -batchEnd 1 -batchSize 100000 -agg-tree-type nary -agg-count 2 -fan-in 2"
                    .split(" ");
        }
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);
        exp(options.master, options.nodeCount, options.type, options.k,
                options.iterations, options.batchStart, options.batchStep,
                options.batchEnd, options.batchSize, options.aggTreeType,
                "generic".equals(options.aggTreeType) ? options.aggCount
                        : options.fanIn, options.dynamic);
    }
}
