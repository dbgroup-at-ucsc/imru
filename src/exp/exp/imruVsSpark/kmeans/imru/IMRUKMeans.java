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
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

import edu.uci.ics.hyracks.imru.api.IIMRUDataGenerator;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.example.utils.Client;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.imruVsSpark.data.DataGenerator;
import exp.imruVsSpark.data.Distribution;
import exp.imruVsSpark.kmeans.SKMeansModel;
import exp.imruVsSpark.kmeans.spark.SparkKMeans;

/**
 * Sparse K-means
 */
public class IMRUKMeans {
    public static void run(boolean memCache,boolean noDiskCache) throws Exception {
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

        File templateDir = new File("exp_data/product_name");
        DataGenerator dataGenerator = new DataGenerator(
                DataGenerator.DEBUG_DATA_POINTS, templateDir);
        SKMeansModel initModel = new SKMeansModel(k, dataGenerator,
                DataGenerator.DEBUG_ITERATIONS);
        SKMeansModel finalModel = Client.run(new SKMeansJob(k,
                dataGenerator.dims), initModel, args);
    }

    static void generateData() throws Exception {
        String cmdline = "";
        if (Client.isServerAvailable(Client.getLocalIp(), 3099)) {
            cmdline += "-host " + Client.getLocalIp() + " -port 3099";
            System.out.println("Connecting to " + Client.getLocalIp());
        } else {
            cmdline += "-host localhost -port 3099 -debug -disable-logging";
            System.out.println("Starting hyracks cluster");
        }

        cmdline += " -example-paths /data/b/data/imru/productName.txt";
        System.out.println("Using command line: " + cmdline);
        String[] args = cmdline.split(" ");

        SKMeansModel finalModel = Client.generateData(new IIMRUDataGenerator() {
            @Override
            public void generate(IMRUContext ctx, OutputStream output)
                    throws IOException {
                try {
                    File templateDir = new File("exp_data/product_name");
                    DataGenerator dataGenerator = new DataGenerator(
                            DataGenerator.DEBUG_DATA_POINTS, templateDir);
                    dataGenerator.generate(false, output);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, args);
        System.exit(0);
    }

    public static void main(String[] args) throws Exception {
//        generateData();
        run(false,true);
        System.exit(0);
    }
}
