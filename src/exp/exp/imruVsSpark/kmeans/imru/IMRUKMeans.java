/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package exp.imruVsSpark.kmeans.imru;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Random;

import edu.uci.ics.hyracks.imru.api.IIMRUDataGenerator;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.example.utils.Client;
import edu.uci.ics.hyracks.imru.example.utils.CreateHar;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.imruVsSpark.data.DataGenerator;
import exp.imruVsSpark.data.Distribution;
import exp.imruVsSpark.kmeans.SKMeansModel;
import exp.imruVsSpark.kmeans.spark.SparkKMeans;

/**
 * Sparse K-means
 */
public class IMRUKMeans {
    public static void run(boolean memCache, boolean noDiskCache)
            throws Exception {
        String cmdline = "";
        if (Client.isServerAvailable(Client.getLocalIp(), 3099)) {
            // hostname of cluster controller
            cmdline += "-host " + Client.getLocalIp() + " -port 3099";
            System.out.println("Connecting to " + Client.getLocalIp());
        } else {
            // debugging mode, everything run in one process
            cmdline += "-host localhost -port 3099 -debug -disable-logging";
            if (memCache)
                cmdline += " -mem-cache";
            if (noDiskCache)
                cmdline += " -no-disk-cache";
            System.out.println("Starting hyracks cluster");
        }

        cmdline += " -example-paths /data/b/data/imru/productName.txt";
        System.out.println("Using command line: " + cmdline);
        String[] args = cmdline.split(" ");

        int k = DataGenerator.DEBUG_K;

        File templateDir = new File(DataGenerator.TEMPLATE);
        DataGenerator dataGenerator = new DataGenerator(
                DataGenerator.DEBUG_DATA_POINTS, templateDir);
        SKMeansModel initModel = new SKMeansModel(k, dataGenerator,
                DataGenerator.DEBUG_ITERATIONS);
        SKMeansModel finalModel = Client.run(new SKMeansJob(k,
                dataGenerator.dims), initModel, args);
        Rt.p("Total examples: " + finalModel.totalExamples);
    }

    public static int runEc2(String cc, int nodes, int size, String path,
            boolean memCache, boolean noDiskCache) throws Exception {
        CreateHar.uploadJarFiles = false;
        DataGenerator.TEMPLATE = "/home/ubuntu/test/exp_data/product_name";
        String cmdline = "";
        cmdline += "-host " + cc + " -port 3099 -frame-size "
                + (16 * 1024 * 1024);
        System.out.println("Connecting to " + Client.getLocalIp());
        //            cmdline += "-host localhost -port 3099 -debug -disable-logging";
        if (memCache)
            cmdline += " -mem-cache";
        if (noDiskCache)
            cmdline += " -no-disk-cache";

        cmdline += " -example-paths ";
        for (int i = 0; i < nodes; i++) {
            if (i > 0)
                cmdline += ",";
            cmdline += "NC" + i + ":" + path;
        }
        System.out.println("Using command line: " + cmdline);
        String[] args = cmdline.split(" ");

        int k = DataGenerator.DEBUG_K;

        File templateDir = new File(DataGenerator.TEMPLATE);
        DataGenerator dataGenerator = new DataGenerator(size, templateDir);
        SKMeansModel initModel = new SKMeansModel(k, dataGenerator,
                DataGenerator.DEBUG_ITERATIONS);
        SKMeansModel finalModel = Client.run(new SKMeansJob(k,
                dataGenerator.dims), initModel, args);
        Rt.p("Total examples: " + finalModel.totalExamples);
        return finalModel.totalExamples;
    }

    public static void generateData(String host, final int count,
            final int splits, final File templateDir, String imruPath,
            final String sparkPath) throws Exception {
        String cmdline = "";
        //        if (Client.isServerAvailable(Client.getLocalIp(), 3099)) {
        //            cmdline += "-host " + Client.getLocalIp() + " -port 3099";
        //            System.out.println("Connecting to " + Client.getLocalIp());
        //        } else {
        cmdline += "-host " + host + " -port 3099";
        System.out.println("Starting hyracks cluster");
        //        }

        CreateHar.uploadJarFiles = false;
        cmdline += " -example-paths ";
        for (int i = 0; i < splits; i++) {
            if (i > 0)
                cmdline += ",";
            cmdline += "NC" + i + ":" + imruPath;
        }
        System.out.println("Using command line: " + cmdline);
        String[] args = cmdline.split(" ");

        Rt.p("generating " + splits + " " + count);
        Client.generateData(new IIMRUDataGenerator() {
            @Override
            public void generate(IMRUContext ctx, OutputStream output)
                    throws IOException {
                try {
                    String nodeId = ctx.getNodeId();
                    if (!nodeId.startsWith("NC"))
                        throw new Error();
                    int id = Integer.parseInt(nodeId.substring(2));
                    PrintStream psSpark = new PrintStream(
                            new BufferedOutputStream(new FileOutputStream(
                                    new File(sparkPath)), 1024 * 1024));
                    PrintStream psImru = new PrintStream(
                            new BufferedOutputStream(output, 1024 * 1024));
                    DataGenerator dataGenerator = new DataGenerator(count
                            * splits, templateDir);
                    for (int i = 0; i < splits; i++) {
                        dataGenerator.generate(false, count, psSpark,
                                i == id ? psImru : null);
                    }
                    psSpark.close();
                    psImru.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, args);
        Rt.p("generate data complete");
    }

    public static void main(String[] args) throws Exception {
        //        generateData(1000, 2);
        run(true, false);
        System.exit(0);
    }
}
