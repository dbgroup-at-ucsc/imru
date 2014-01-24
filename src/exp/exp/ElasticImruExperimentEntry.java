package exp;

import java.io.File;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.imru.example.utils.Client;
import edu.uci.ics.hyracks.imru.example.utils.Client.Options;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.imruVsSpark.data.DataGenerator;
import exp.imruVsSpark.kmeans.SKMeansModel;
import exp.imruVsSpark.kmeans.imru.IMRUKMeans;
import exp.imruVsSpark.kmeans.spark.SparkKMeans;
import exp.imruVsSpark.kmeans.stratosphere.StratosphereKMeans;
import exp.imruVsSpark.lr.imru.ImruLRMain;
import exp.test0.GnuPlot;
import exp.types.ImruExpParameters;

public class ElasticImruExperimentEntry {
    public static String getDataPath(int sizePerNode, int nodeCount) {
        return "/data/size" + sizePerNode + "/nodes" + nodeCount + "/data.txt";
    }

    public static void exp(ImruExpParameters p) throws Exception {
        String user = "ubuntu";
        //        Client.disableLogging();
        DataGenerator.TEMPLATE = "/home/ubuntu/test/exp_data/product_name";
        if (!new File(DataGenerator.TEMPLATE).exists()) {
            user = "wangrui";
            DataGenerator.TEMPLATE = "/home/wangrui/test/exp_data/product_name";
        }
        File resultDir = new File("result");
        resultDir.mkdir();
        GnuPlot plot = new GnuPlot(resultDir, "kmeans" + p.method,
                "Data points (10^5)", "Time (seconds)");
        plot.extra = "set title \"K=" + p.k + ",Iteration=" + p.iterations
                + "\"";
        plot.setPlotNames(p.method, "data");
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
                    * p.nodeCount, templateDir);
            final SKMeansModel model = new SKMeansModel(p.k, dataGenerator, 20);
            byte[] bs = JavaSerializationUtils.serialize(model);
            Rt.p("Max model size: %,d", bs.length);
        }
        for (int sizePerNode = p.batchStart; sizePerNode <= p.batchEnd; sizePerNode += p.batchStep) {
            int pointPerNode = sizePerNode * p.batchSize;
            int dataSize = pointPerNode * p.nodeCount;

            long start = System.currentTimeMillis();
            p.path = getDataPath(sizePerNode, p.nodeCount);
            //            Rt.p("generating data");
            //            DataGenerator.main(new String[] { "/home/ubuntu/test/data.txt" });
            //            IMRUKMeans.generateData(master, pointPerNode,
            //                    nodeCount,new File(DataGenerator.TEMPLATE),);
            //            long dataTime = System.currentTimeMillis() - start;

            //            start = System.currentTimeMillis();
            //            SparseKMeans.run();
            //            long bareTime = System.currentTimeMillis() - start;
            if ("imruDisk".equals(p.method)) {
                Rt.p("running IMRU in disk " + sizePerNode);
                start = System.currentTimeMillis();
                p.memCache = false;
                p.noDiskCache = false;
                int processed2 = -1;
                if ("kmeans".equals(p.experiment))
                    processed2 = IMRUKMeans.runVM(p);
                else if ("lr".equals(p.experiment))
                    processed2 = ImruLRMain.runVM(p);
                else
                    throw new Error(p.experiment);
                long imruDiskTime = System.currentTimeMillis() - start;
                plot.startNewX(pointPerNode / 100000);
                //                plot.addY(dataTime / 1000.0);
                plot.addY(imruDiskTime / 1000.0);
                plot.addY(processed2);
            } else if ("imruMem".equals(p.method)) {
                Rt.p("running IMRU in memory " + sizePerNode);
                p.memCache = true;
                p.noDiskCache = false;
                start = System.currentTimeMillis();
                int processed1 = -1;
                if ("kmeans".equals(p.experiment))
                    processed1 = IMRUKMeans.runVM(p);
                else if ("lr".equals(p.experiment))
                    processed1 = ImruLRMain.runVM(p);
                else
                    throw new Error(p.experiment);
                long imruMemTime = System.currentTimeMillis() - start;

                //            start = System.currentTimeMillis();
                //            IMRUKMeans.runEc2(master, false, true);
                //            long imruParseTime = System.currentTimeMillis() - start;
                plot.startNewX(pointPerNode / 100000);
                //                plot.addY(dataTime / 1000.0);
                plot.addY(imruMemTime / 1000.0);
                plot.addY(processed1);
            } else if ("spark".equals(p.method)) {
                Rt.p("running spark " + sizePerNode);
                start = System.currentTimeMillis();
                String path = getDataPath(sizePerNode, p.nodeCount);
                int processed = -1;
                if ("kmeans".equals(p.experiment))
                    processed = SparkKMeans.run(p.master, dataSize, "/home/"
                            + user + "/spark-0.8.0-incubating", path,
                            p.nodeCount, p.k, p.iterations);
                else
                    throw new Error(p.experiment);
                long sparkTime = System.currentTimeMillis() - start;
                plot.startNewX(pointPerNode / 100000);
                //                plot.addY(dataTime / 1000.0);
                plot.addY(sparkTime / 1000.0);
                plot.addY(processed);
            } else if ("stratosphere".equals(p.method)) {
                Rt.p("running stratosphere " + sizePerNode);
                start = System.currentTimeMillis();
                String path = getDataPath(sizePerNode, p.nodeCount);
                int processed = -1;
                if ("kmeans".equals(p.experiment))
                    processed = StratosphereKMeans.run(p.master, dataSize,
                            "/home/" + user + "/spark-0.8.0-incubating", path,
                            p.nodeCount, p.k, p.iterations);
                else
                    throw new Error(p.experiment);
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
        @Option(name = "-method", required = true)
        public String method;
        @Option(name = "-experiment", required = true)
        public String experiment;
        @Option(name = "-master", required = true)
        public String master;
        @Option(name = "-nodeCount", required = true)
        public int nodeCount;
        @Option(name = "-k", required = true)
        public int k;
        @Option(name = "-iterations", required = true)
        public int iterations;
        @Option(name = "-batchStart", required = true)
        public int batchStart;
        @Option(name = "-batchEnd", required = true)
        public int batchEnd;
        @Option(name = "-batchStep", required = true)
        public int batchStep;
        @Option(name = "-batchSize", required = true)
        public int batchSize;
        @Option(name = "-agg-tree-type", usage = "The aggregation tree type (none, nary, or generic)")
        public String aggTreeType;
        @Option(name = "-agg-count", usage = "The number of aggregators to use, if using an aggregation tree")
        public int aggCount = -1;
        @Option(name = "-fan-in", usage = "The fan-in, if using an nary aggregation tree")
        public int fanIn = -1;
        @Option(name = "-dynamic")
        public boolean dynamic;
        @Option(name = "-dynamic-disable")
        public boolean dynamicDisable;
        @Option(name = "-dynamic-debug")
        public boolean dynamicDebug;
        @Option(name = "-straggler")
        public int straggler;
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            args = "-master 192.168.56.104 -nodeCount 2 -type imruMem -k 6 -iterations 1 -batchStart 1 -batchStep 3 -batchEnd 1 -batchSize 100000 -agg-tree-type nary -agg-count 2 -fan-in 2"
                    .split(" ");
        }
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);
        exp(new ImruExpParameters(options));
    }
}
