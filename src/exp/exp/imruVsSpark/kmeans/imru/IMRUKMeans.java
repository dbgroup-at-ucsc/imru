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

import java.io.File;

import edu.uci.ics.hyracks.imru.util.Client;
import edu.uci.ics.hyracks.imru.util.CreateDeployment;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.imruVsSpark.data.DataGenerator;
import exp.imruVsSpark.kmeans.SKMeansModel;
import exp.types.ImruExpParameters;

/**
 * Sparse K-means
 */
public class IMRUKMeans {
    public static void run(boolean memCache, boolean noDiskCache, int k,
            int iterations, int dataPoints, int numOfDimensions, int stragger)
            throws Exception {
        String cmdline = "";
        if (Client.isServerAvailable(Client.getLocalIp(), 3099)) {
            // hostname of cluster controller
            cmdline += "-host " + Client.getLocalIp() + " -port 3099";
            System.out.println("Connecting to " + Client.getLocalIp());
        } else {
            // debugging mode, everything run in one process
            cmdline += "-host localhost -port 3099 -debugNodes 8 -debug -disable-logging";
            if (memCache)
                cmdline += " -mem-cache";
            if (noDiskCache)
                cmdline += " -no-disk-cache";
            cmdline += " -dynamic";
            cmdline += " -dynamic-swap-time 0";
            //            cmdline += " -dynamic-debug";
            System.out.println("Starting hyracks cluster");
        }

        cmdline += " -agg-tree-type nary -fan-in 2";
        //        cmdline += " -frame-size " + (16 * 1024 * 1024);
        cmdline += " -input-paths" + " NC0:/data/b/data/imru/productName.txt,"
                + "NC1:/data/b/data/imru/productName.txt,"
                + "NC2:/data/b/data/imru/productName.txt,"
                + "NC3:/data/b/data/imru/productName.txt,"
                + "NC4:/data/b/data/imru/productName.txt,"
                + "NC5:/data/b/data/imru/productName.txt,"
                + "NC6:/data/b/data/imru/productName.txt,"
                + "NC7:/data/b/data/imru/productName.txt";
        System.out.println("Using command line: " + cmdline);
        String[] args = cmdline.split(" ");

        File templateDir = new File(DataGenerator.TEMPLATE);
        DataGenerator dataGenerator = new DataGenerator(dataPoints,
                numOfDimensions, templateDir);
        SKMeansModel initModel = new SKMeansModel(k, dataGenerator, iterations);
        SKMeansModel finalModel = Client.run(new SKMeansJob(null, k,
                dataGenerator.numOfDims, stragger), initModel, args);
        Rt.p("Total examples: " + finalModel.totalExamples);
    }

    public static int runVM(ImruExpParameters p) throws Exception {
        CreateDeployment.uploadJarFiles = false;
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
        return finalModel.totalExamples;
    }

    //    public static void generateData(String host, final int count,
    //            final int splits, final File templateDir, String imruPath,
    //            final String sparkPath) throws Exception {
    //        String cmdline = "";
    //        //        if (Client.isServerAvailable(Client.getLocalIp(), 3099)) {
    //        //            cmdline += "-host " + Client.getLocalIp() + " -port 3099";
    //        //            System.out.println("Connecting to " + Client.getLocalIp());
    //        //        } else {
    //        cmdline += "-host " + host + " -port 3099";
    //        System.out.println("Starting hyracks cluster");
    //        //        }
    //
    //        CreateDeployment.uploadJarFiles = false;
    //        cmdline += " -input-paths ";
    //        for (int i = 0; i < splits; i++) {
    //            if (i > 0)
    //                cmdline += ",";
    //            cmdline += String.format("NC" + i + ":" + imruPath, i);
    //        }
    //        System.out.println("Using command line: " + cmdline);
    //        String[] args = cmdline.split(" ");
    //
    //        Rt.p("generating " + splits + " " + count);
    //        Client.generateData(new IIMRUDataGenerator() {
    //            @Override
    //            public void generate(IMRUContext ctx, OutputStream output)
    //                    throws IOException {
    //                try {
    //                    String nodeId = ctx.getNodeId();
    //                    if (!nodeId.startsWith("NC"))
    //                        throw new Error();
    //                    int id = Integer.parseInt(nodeId.substring(2));
    //                    PrintStream psSpark = new PrintStream(
    //                            new BufferedOutputStream(new FileOutputStream(
    //                                    new File(sparkPath)), 1024 * 1024));
    //                    PrintStream psImru = new PrintStream(
    //                            new BufferedOutputStream(output, 1024 * 1024));
    //                    DataGenerator dataGenerator = new DataGenerator(count
    //                            * splits, templateDir);
    //                    for (int i = 0; i < splits; i++) {
    //                        dataGenerator.generate(false, count, psSpark,
    //                                i == id ? psImru : null);
    //                    }
    //                    psSpark.close();
    //                    psImru.close();
    //                } catch (Exception e) {
    //                    e.printStackTrace();
    //                }
    //            }
    //        }, args);
    //        Rt.p("generate data complete");
    //    }

    public static void main(String[] args) throws Exception {
        //        generateData(1000, 2);
        try {
            //            ImruSendOperator.debug=true;
            run(true, false, 1, 1, 10, 1000000,5000);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        System.exit(0);
    }
}
