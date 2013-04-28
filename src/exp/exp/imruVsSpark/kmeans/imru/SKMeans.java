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
import java.util.Random;

import edu.uci.ics.hyracks.imru.example.utils.Client;
import exp.imruVsSpark.data.DataGenerator;
import exp.imruVsSpark.data.Distribution;

/**
 * Sparse K-means
 */
public class SKMeans {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            // if no argument is given, the following code
            // creates default arguments to run the example
            String cmdline = "";
            if (Client.isServerAvailable(Client.getLocalIp(), 3099)) {
                // hostname of cluster controller
                cmdline += "-host " + Client.getLocalIp() + " -port 3099";
                System.out.println("Connecting to " + Client.getLocalIp());
            } else {
                // debugging mode, everything run in one process
                cmdline += "-host localhost -port 3099 -debug -disable-logging";
                System.out.println("Starting hyracks cluster");
            }

            cmdline += " -example-paths /data/b/data/imru/productName.txt";
            System.out.println("Using command line: " + cmdline);
            args = cmdline.split(" ");
        }

        int k = 3;

        File templateDir = new File("exp_data/product_name");
        DataGenerator dataGenerator = new DataGenerator(51200, templateDir);
        SKMeansModel initModel = new SKMeansModel(k, dataGenerator, 20);
        SKMeansModel finalModel = Client.run(new SKMeansJob(k, dataGenerator.dims), initModel, args);
        System.exit(0);
    }
}
