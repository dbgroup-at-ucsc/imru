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

package edu.uci.ics.hyracks.imru.example.dynamic;

import edu.uci.ics.hyracks.imru.util.Client;

/**
 * Demonstration of IMRU dynamic swapping
 */
public class TestDynamicAggr {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            // if no argument is given, the following code
            // creates default arguments to run the example
            String cmdline = "";
            int totalNodes = 5;
            boolean useExistingCluster = Client.isServerAvailable(Client
                    .getLocalIp(), 3099);
            if (useExistingCluster) {
                // hostname of cluster controller
                String ip = Client.getLocalIp();
                cmdline += "-host " + ip + " -port 3099";
                System.out.println("Connecting to " + Client.getLocalIp());
            } else {
                // debugging mode, everything run in one process
                cmdline += "-host localhost -port 3099 -debug -disable-logging";
                cmdline += " -debugNodes " + totalNodes;
                cmdline += " -agg-tree-type nary -fan-in 2";
                //                cmdline += " -agg-tree-type none";
                cmdline += " -compress-after-iterations 2";
                cmdline += " -disable-logging";
                cmdline += " -dynamic";
                cmdline += " -dynamic-swap-time 0";
//                cmdline += " -dynamic-disable";

                System.out.println("Starting hyracks cluster");
            }

            String exampleData = System.getProperty("user.home")
                    + "/hyracks/imru/imru-example/data/helloworld";
            cmdline += " -input-paths " + exampleData + "/hello0.txt";
            for (int i = 1; i < totalNodes; i++)
                cmdline += "," + exampleData + "/hello" + i + ".txt";
            System.out.println("Using command line: " + cmdline);
            args = cmdline.split(" ");
        }

        try {
            int modelSize = 256 * 1024;
            byte[] finalModel = Client.run(new AggrJob(modelSize, 1),
                    new byte[modelSize], args);
            System.out.println("FinalModel: " + finalModel.length);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(0);
        }
        System.exit(0);
    }
}
