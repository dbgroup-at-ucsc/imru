package exp.test0.imruTest;

import java.io.File;
import java.io.InputStream;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import edu.uci.ics.hyracks.imru.example.helloworld.HelloWorldJob;
import edu.uci.ics.hyracks.imru.file.ConfigurationFactory;
import edu.uci.ics.hyracks.imru.file.HDFSSplit;
import edu.uci.ics.hyracks.imru.util.Client;
import edu.uci.ics.hyracks.imru.util.Rt;

public class TestHdfs {
    static File generateData() throws Exception {
        byte[] bs = new byte[100];
        for (int i = 0; i < bs.length; i += 2) {
            bs[i] = (byte) ('a' + (i / 20));
            bs[i + 1] = (byte) ('0' + (i / 2 % 10));
        }
        File file = new File("/tmp/cache/testhdfs.txt");
        Rt.write(file, bs);
        return file;
    }

    static void testHdfs() throws Exception {
        generateData();
        Configuration hadoopConf = new Configuration();
        ConfigurationFactory confFactory = new ConfigurationFactory("");
        FileSystem dfs = FileSystem.get(confFactory.createConfiguration());
        {
            List<HDFSSplit> splits = HDFSSplit.get(confFactory, new String[] {
                    "/tmp/cache/testhdfs.txt", "/tmp/cache/testhdfs.txt", }, 1,
                    0, 20);
            Rt.p(splits.size());
            for (int i = 0; i < splits.size(); i++) {
                InputStream is = splits.get(i).getInputStream();
                byte[] bs2 = Rt.read(is);
                Rt.p(new String(bs2));
            }
        }
        {
            List<HDFSSplit> splits = HDFSSplit.get(confFactory, new String[] {
                    "/tmp/cache/testhdfs.txt", "/tmp/cache/testhdfs.txt", }, 1,
                    0, 1024 * 1024);
            Rt.p(splits.size());
            for (int i = 0; i < splits.size(); i++) {
                InputStream is = splits.get(i).getInputStream();
                byte[] bs2 = Rt.read(is);
                Rt.p(new String(bs2));
            }
        }
        {
            List<HDFSSplit> splits = HDFSSplit.get(confFactory,
                    new String[] { "/home/wangrui/b/data/imru/spark1.txt", },
                    1, 0, 1024 * 1024 + 1);
            Rt.p(splits.size());
            for (int i = 0; i < 2; i++) {
                InputStream is = splits.get(i).getInputStream();
                byte[] bs2 = Rt.read(is);
                Rt.p(bs2.length);
            }
        }
    }

    public static void testImruHDFS() throws Exception {
        //        generateData();
        byte[] bs = new byte[5];
        for (int i = 0; i < bs.length; i++) {
            bs[i] = (byte) ('a' + i);
        }
        File file = new File("/tmp/cache/testhdfs.txt");
        Rt.write(file, bs);
        // if no argument is given, the following code
        // creates default arguments to run the example
        String cmdline = "";
        int totalNodes = 8;
        // debugging mode, everything run in one process
        cmdline += "-host localhost -port 3099 -debug -disable-logging";
        cmdline += " -debugNodes " + totalNodes;
        cmdline += " -num-split 3";
        cmdline += " -min-split-size 1";
        cmdline += " -max-split-size 1";
        cmdline += " -frame-size 256";

        System.out.println("Starting hyracks cluster");
        cmdline += " -input-paths " + file.getAbsolutePath() + "?id=0";
//                + file.getAbsolutePath() + "?id=1";
        System.out.println("Using command line: " + cmdline);
        String[] args = cmdline.split(" ");

        try {
            String finalModel = Client.run(new HelloWorldJob(), "", args);
            System.out.println("FinalModel: " + finalModel);
        } catch (Throwable e) {
            Rt.p("failed");
            e.printStackTrace();
            System.exit(0);
        }
        System.exit(0);
    }

    public static void main(String[] args) throws Exception {
        //        testHdfs();
        testImruHDFS();
    }
}
