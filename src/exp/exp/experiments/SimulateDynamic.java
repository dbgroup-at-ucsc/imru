package exp.experiments;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

import edu.uci.ics.hyracks.imru.api.DataWriter;
import edu.uci.ics.hyracks.imru.api.FrameWriter;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api.IMRUDataException;
import edu.uci.ics.hyracks.imru.api.IMRUMapContext;
import edu.uci.ics.hyracks.imru.api.IMRUReduceContext;
import edu.uci.ics.hyracks.imru.api.ImruFrames;
import edu.uci.ics.hyracks.imru.api.ImruIterInfo;
import edu.uci.ics.hyracks.imru.api.ImruObject;
import edu.uci.ics.hyracks.imru.api.ImruOptions;
import edu.uci.ics.hyracks.imru.api.ImruStream;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.test.DynamicMappingFunctionalTest;
import edu.uci.ics.hyracks.imru.util.Client;
import edu.uci.ics.hyracks.imru.util.Rt;
import edu.uci.ics.hyracks.imru.wrapper.IMRUMultiCore;
import exp.ImruExpFigs;
import exp.imruVsSpark.data.DataGenerator;
import exp.imruVsSpark.kmeans.SKMeansModel;
import exp.imruVsSpark.kmeans.imru.SKMeansJob;
import exp.test0.GnuPlot;
import exp.types.ImruExpParameters;

public class SimulateDynamic {
    public static int msPerSplit = 1000;

    public static class Job extends ImruObject<byte[], byte[], byte[]> {
        int straggler;
        int modelSize;
        int iterations;

        public Job(int straggler, int modelSize, int iterations) {
            this.straggler = straggler;
            this.modelSize = modelSize;
            this.iterations = iterations;
        }

        @Override
        public void parse(IMRUContext ctx, InputStream input,
                DataWriter<byte[]> output) throws IOException {
            output.addData(new byte[0]);
        }

        @Override
        public byte[] map(IMRUContext ctx, Iterator<byte[]> input, byte[] model)
                throws IOException {
            while (input.hasNext())
                input.next();
            Rt.sleep(msPerSplit);
            if (straggler > 0 && ctx.getPartition() == ctx.getPartitions() - 1)
                Rt.sleep(straggler);
            return new byte[modelSize];
        }

        @Override
        public byte[] reduce(IMRUContext ctx, Iterator<byte[]> input)
                throws IMRUDataException {
            byte[] data = null;
            while (input.hasNext())
                data = input.next();
            if (data.length != modelSize)
                throw new Error();
            return data;
        }

        @Override
        public byte[] update(IMRUContext ctx, Iterator<byte[]> input,
                byte[] model) throws IMRUDataException {
            while (input.hasNext())
                input.next();
            iterations--;
            return model;
        }

        @Override
        public boolean shouldTerminate(byte[] model, ImruIterInfo info) {
            //            info.printReport();
            return iterations <= 0;
        }
    };

    static void kmeans() throws Exception {
        IMRUMultiCore.networkSpeedInternal = 1024 * 1024;

        ImruOptions options = new ImruOptions();
        options.inputPaths = "data/kmeans/kmeans0.txt";
        options.numOfNodes = 3;
        options.splitsPerNode = 10;
        options.memCache = true;
        options.modelFilename = "model";
        options.dynamicAggr = true;
        options.dynamicMapping = true;
        options.dynamicDisableRelocation = false;
        //        options.dynamicDebug = true;
        options.disableLogging = true;
        ImruExpParameters p = Dynamic.parameters(false);

        DataGenerator.TEMPLATE = "/home/ubuntu/test/exp_data/product_name";
        if (!new File(DataGenerator.TEMPLATE).exists())
            DataGenerator.TEMPLATE = "/home/wangrui/test/exp_data/product_name";
        System.out.println("Connecting to " + Client.getLocalIp());
        File templateDir = new File(DataGenerator.TEMPLATE);
        DataGenerator dataGenerator = new DataGenerator(p.dataSize,
                p.numOfDimensions, templateDir);
        SKMeansModel initModel = new SKMeansModel(p.k, dataGenerator,
                p.iterations);
        SKMeansModel finalModel = Client.run(new SKMeansJob(p.logDir, p.k,
                dataGenerator.numOfDims, p.straggler), initModel, p
                .getClientOptions());
        Rt.p("Total examples: " + finalModel.totalExamples);

        //        ImruStream<String, String> job = new SKMeansJob();
        //        String finalModel = IMRUMultiCore.run(options, model, job);
        //        System.out.println("FinalModel: " + finalModel);
        //        System.exit(0);
    }

    public static void dynamicMapping(String[] args) throws Exception {
        IMRUMultiCore.networkSpeedInternal = 100 * 1024 * 1024;
        ImruOptions options = new ImruOptions();
        options.inputPaths = "/data/data/batch100000/10/nodes8/dims100000.txt";
        msPerSplit = 1000;
        options.numOfNodes = 5;
        options.splitsPerNode = 5;
        options.memCache = true;
        options.modelFilename = "model";
        options.dynamicAggr = true;
        options.dynamicMapping = true;
        options.dynamicDisableRelocation = false;
        options.dynamicDisableSwapping = true;
        options.disableLogging = true;
        {
            Rt.p("warm up");
            ImruStream<byte[], byte[]> job = new Job(0, 1024 * 1024, 1);
            byte[] finalModel = IMRUMultiCore.run(options,
                    new byte[1024 * 1024], job);
            System.out.println("FinalModel: " + finalModel.length);
        }
        options.numOfNodes = 32;
        options.splitsPerNode = 10;
        int mb = 1024 * 1024;
        int iterations = 1;
        for (int si = 0; si < 2; si++) {
            int stragger = si == 0 ? 0 : msPerSplit * 9;
            if (false) {
                GnuPlot plot = new GnuPlot(new File("/tmp/cache"), "ModelSize_"
                        + (si == 0 ? "Normal" : "stragger"), "Model size (MB)",
                        "Time (seconds)");
                plot.extra = "set title \"" + options.numOfNodes + " nodes "
                        + options.splitsPerNode + " splits/node "
                        + String.format("%.2f s/split ", msPerSplit / 1000.0)
                        + iterations + " iterations "
                        + (IMRUMultiCore.networkSpeedInternal / mb)
                        + " MB network" + " \"";
                plot.setPlotNames("Fixed mapping", "Dynamic mapping");
                plot.startPointType = 1;
                plot.pointSize = 1;
                plot.scale = false;
                plot.colored = true;
                for (int modelSize = 1; modelSize <= 10; modelSize++) {
                    byte[] model = new byte[modelSize * mb];
                    double[] times = new double[2];
                    for (int i = 0; i < 2; i++) {
                        Rt.p("Model size: " + modelSize);
                        options.dynamicDisableRelocation = (i == 0);
                        long start = System.currentTimeMillis();
                        ImruStream<byte[], byte[]> job = new Job(stragger,
                                modelSize * mb, iterations);
                        byte[] finalModel = IMRUMultiCore.run(options, model,
                                job);
                        System.out.println("FinalModel: " + finalModel.length);
                        times[i] = (System.currentTimeMillis() - start) / 1000.0;
                    }
                    plot.startNewX(modelSize);
                    plot.addY(times[0]);
                    plot.addY(times[1]);
                    plot.finish();
                }
            } else if (false) {
                int modelSize = 1;
                GnuPlot plot = new GnuPlot(new File("/tmp/cache"), "Nodes_"
                        + (si == 0 ? "Normal" : "stragger"), "Number of nodes",
                        "Time (seconds)");
                plot.extra = "set title \"" + modelSize + "MB model "
                        + options.splitsPerNode + " splits/node "
                        + String.format("%.2f s/split ", msPerSplit / 1000.0)
                        + iterations + " iterations "
                        + (IMRUMultiCore.networkSpeedInternal / mb)
                        + " MB network" + " \"";
                plot.setPlotNames("Fixed mapping", "Dynamic mapping");
                plot.startPointType = 1;
                plot.pointSize = 1;
                plot.scale = false;
                plot.colored = true;
                for (options.numOfNodes = 2; options.numOfNodes <= 64; options.numOfNodes *= 2) {
                    byte[] model = new byte[modelSize * mb];
                    double[] times = new double[2];
                    for (int i = 0; i < 2; i++) {
                        Rt.p("Nodes: " + options.numOfNodes);
                        options.dynamicDisableRelocation = (i == 0);
                        long start = System.currentTimeMillis();
                        ImruStream<byte[], byte[]> job = new Job(stragger,
                                modelSize * mb, iterations);
                        byte[] finalModel = IMRUMultiCore.run(options, model,
                                job);
                        System.out.println("FinalModel: " + finalModel.length);
                        times[i] = (System.currentTimeMillis() - start) / 1000.0;
                    }
                    plot.startNewX(options.numOfNodes);
                    plot.addY(times[0]);
                    plot.addY(times[1]);
                    plot.finish();
                }
            } else {
                int modelSize = 1;
                GnuPlot plot = new GnuPlot(new File("/tmp/cache"), "Network_"
                        + (si == 0 ? "Normal" : "stragger"),
                        "Network speed (MB)", "Time (seconds)");
                plot.extra = "set title \"" + options.numOfNodes + " nodes "
                        + modelSize + "MB model " + options.splitsPerNode
                        + " splits/node "
                        + String.format("%.2f s/split ", msPerSplit / 1000.0)
                        + iterations + " iterations " + " \"";
                plot.setPlotNames("Fixed mapping", "Dynamic mapping");
                plot.startPointType = 1;
                plot.pointSize = 1;
                plot.scale = false;
                plot.colored = true;
                for (int speedMb = 1; speedMb <= 1000; speedMb *= 10) {
                    IMRUMultiCore.networkSpeedInternal = speedMb * mb;
                    byte[] model = new byte[modelSize * mb];
                    double[] times = new double[2];
                    for (int i = 0; i < 2; i++) {
                        Rt.p("Network speed: "
                                + IMRUMultiCore.networkSpeedInternal);
                        options.dynamicDisableRelocation = (i == 0);
                        long start = System.currentTimeMillis();
                        ImruStream<byte[], byte[]> job = new Job(stragger,
                                modelSize * mb, iterations);
                        byte[] finalModel = IMRUMultiCore.run(options, model,
                                job);
                        System.out.println("FinalModel: " + finalModel.length);
                        times[i] = (System.currentTimeMillis() - start) / 1000.0;
                    }
                    plot.startNewX(speedMb);
                    plot.addY(times[0]);
                    plot.addY(times[1]);
                    plot.finish();
                }
            }
        }
        System.exit(0);
    }

    public static void dynamicAggr(String[] args) throws Exception {
        IMRUMultiCore.networkSpeedInternal = 100 * 1024 * 1024;
        ImruOptions options = new ImruOptions();
        options.inputPaths = "/data/data/batch100000/10/nodes8/dims100000.txt";
        msPerSplit = 0;
        options.numOfNodes = 32;
        options.splitsPerNode = 1;
        options.memCache = true;
        options.modelFilename = "model";
        options.dynamicAggr = true;
        options.dynamicMapping = true;
        options.dynamicDisableRelocation = true;
        options.dynamicDisableSwapping = false;
        options.disableLogging = true;
        {
            Rt.p("warm up");
            ImruStream<byte[], byte[]> job = new Job(0, 1024 * 1024, 1);
            byte[] finalModel = IMRUMultiCore.run(options,
                    new byte[1024 * 1024], job);
            System.out.println("FinalModel: " + finalModel.length);
        }
        options.numOfNodes = 32;
        options.splitsPerNode = 1;
        int mb = 1024 * 1024;
        int iterations = 1;
        for (int si = 0; si < 2; si++) {
            int stragger = si == 0 ? 0 : 5;
            if (true) {
                GnuPlot plot = new GnuPlot(new File("/tmp/cache"),
                        "aggrModelSize_" + (si == 0 ? "Normal" : "stragger"),
                        "Model size (MB)", "Time (seconds)");
                plot.extra = "set title \"" + options.numOfNodes + " nodes "
                        + (IMRUMultiCore.networkSpeedInternal / mb)
                        + " MB network" + " \"";
                plot.setPlotNames("Fixed aggregation", "Dynamic aggregation");
                plot.startPointType = 1;
                plot.pointSize = 1;
                plot.scale = false;
                plot.colored = true;
                for (int modelSize = 10; modelSize <= 100; modelSize += 10) {
                    byte[] model = new byte[modelSize * mb];
                    double[] times = new double[2];
                    for (int i = 0; i < 2; i++) {
                        Rt.p("Model size: " + modelSize);
                        options.dynamicDisableRelocation = (i == 0);
                        long start = System.currentTimeMillis();
                        ImruStream<byte[], byte[]> job = new Job(stragger,
                                modelSize * mb, iterations);
                        byte[] finalModel = IMRUMultiCore.run(options, model,
                                job);
                        System.out.println("FinalModel: " + finalModel.length);
                        times[i] = (System.currentTimeMillis() - start) / 1000.0;
                    }
                    plot.startNewX(modelSize);
                    plot.addY(times[0]);
                    plot.addY(times[1]);
                    plot.finish();
                }
            }
            if (true) {
                int modelSize = 100;
                GnuPlot plot = new GnuPlot(new File("/tmp/cache"), "aggrNodes_"
                        + (si == 0 ? "Normal" : "stragger"), "Number of nodes",
                        "Time (seconds)");
                plot.extra = "set title \"" + modelSize + "MB model "
                        + (IMRUMultiCore.networkSpeedInternal / mb)
                        + " MB network" + " \"";
                plot.setPlotNames("Fixed aggregation", "Dynamic aggregation");
                plot.startPointType = 1;
                plot.pointSize = 1;
                plot.scale = false;
                plot.colored = true;
                for (options.numOfNodes = 2; options.numOfNodes <= 64; options.numOfNodes *= 2) {
                    byte[] model = new byte[modelSize * mb];
                    double[] times = new double[2];
                    for (int i = 0; i < 2; i++) {
                        Rt.p("Nodes: " + options.numOfNodes);
                        options.dynamicDisableRelocation = (i == 0);
                        long start = System.currentTimeMillis();
                        ImruStream<byte[], byte[]> job = new Job(stragger,
                                modelSize * mb, iterations);
                        byte[] finalModel = IMRUMultiCore.run(options, model,
                                job);
                        System.out.println("FinalModel: " + finalModel.length);
                        times[i] = (System.currentTimeMillis() - start) / 1000.0;
                    }
                    plot.startNewX(options.numOfNodes);
                    plot.addY(times[0]);
                    plot.addY(times[1]);
                    plot.finish();
                }
            }
            {
                int modelSize = 100;
                GnuPlot plot = new GnuPlot(new File("/tmp/cache"),
                        "aggrNetwork_" + (si == 0 ? "Normal" : "stragger"),
                        "Network speed (MB)", "Time (seconds)");
                plot.extra = "set title \"" + options.numOfNodes + " nodes "
                        + modelSize + "MB model " + " \"";
                plot.setPlotNames("Fixed aggregation", "Dynamic aggregation");
                plot.startPointType = 1;
                plot.pointSize = 1;
                plot.scale = false;
                plot.colored = true;
                for (int speedMb = 1; speedMb <= 1000; speedMb *= 10) {
                    IMRUMultiCore.networkSpeedInternal = speedMb * mb;
                    byte[] model = new byte[modelSize * mb];
                    double[] times = new double[2];
                    for (int i = 0; i < 2; i++) {
                        Rt.p("Network speed: "
                                + IMRUMultiCore.networkSpeedInternal);
                        options.dynamicDisableRelocation = (i == 0);
                        long start = System.currentTimeMillis();
                        ImruStream<byte[], byte[]> job = new Job(stragger,
                                modelSize * mb, iterations);
                        byte[] finalModel = IMRUMultiCore.run(options, model,
                                job);
                        System.out.println("FinalModel: " + finalModel.length);
                        times[i] = (System.currentTimeMillis() - start) / 1000.0;
                    }
                    plot.startNewX(speedMb);
                    plot.addY(times[0]);
                    plot.addY(times[1]);
                    plot.finish();
                }
            }
        }
        System.exit(0);
    }

    public static void main(String[] args) throws Exception {
        //        dynamicMapping(args);
        dynamicAggr(args);
    }
}
