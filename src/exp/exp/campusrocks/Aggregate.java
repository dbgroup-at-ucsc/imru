package exp.campusrocks;

import edu.uci.ics.hyracks.imru.util.Client;

public class Aggregate {
    public static void main(String[] args) throws Exception {
        int totalNodes = Integer.parseInt(args[0]);
        int fanin = Integer.parseInt(args[1]);
        int modelSize = Integer.parseInt(args[2]);
        String tmpDir = args[3];
        // if no argument is given, the following code
        // creates default arguments to run the example
        String cmdline = "";
        // debugging mode, everything run in one process
        cmdline += "-host localhost -port 3099 -debug -disable-logging";
        cmdline += " -debugNodes " + totalNodes;
        cmdline += " -agg-tree-type nary -fan-in " + fanin;
        //                    cmdline += " -agg-tree-type none";
        cmdline += " -nc-temp-path " + tmpDir;

        System.out.println("Starting hyracks cluster");

        String exampleData = "data/helloworld";
        cmdline += " -input-paths NC0:" + exampleData + "/hello0.txt";
        for (int i = 1; i < totalNodes; i++)
            cmdline += ",NC" + (i % totalNodes) + ":" + exampleData
                    + "/hello0.txt";
        System.out.println("Using command line: " + cmdline);
        args = cmdline.split(" ");

        try {
            byte[] finalModel = Client.run(new AggregateJob(modelSize),
                    new byte[0], args);
            System.out.println("FinalModel: " + finalModel.length);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(0);
        }
        System.exit(0);
    }
}
