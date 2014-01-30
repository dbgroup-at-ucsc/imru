package exp.campusrocks;

import java.io.File;

import edu.uci.ics.hyracks.imru.util.Client;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.test0.GnuPlot;

public class CampusRocksExperiment {
    public static void main(String[] args) throws Exception {
        int totalNodes = 16;
        if (args.length > 0)
            totalNodes = Integer.parseInt(args[0]);
        String currentDir = new File("").getAbsolutePath();
        File tmpDir;
        if ("/data/a/imru/ucscImru".equals(currentDir)) {
            tmpDir = new File("/tmp/cache");
        } else {
            tmpDir = new File("tmp");
        }
        GnuPlot plot = new GnuPlot(tmpDir, "fanInAndModelSize", "fan-in",
                "Time (seconds)");
        plot.setPlotNames("10MB", "20MB", "40MB", "80MB", "160MB", "320MB");
        plot.startPointType = 1;
        plot.pointSize = 1;
        plot.scale = false;
        plot.colored = true;
        for (int fanin = 2; fanin <= 6; fanin++) {
            plot.startNewX(fanin);
            for (int modelMb = 10; modelMb <= 320; modelMb *= 2) {
                Rt.p(fanin + " " + modelMb);
                int modelSize = modelMb * 1024 * 1024;
                long startTime = System.nanoTime();
                Rt.runAndShowCommand("bash run.sh exp.campusrocks.Aggregate "
                        + totalNodes + " " + fanin + " " + modelSize + " "
                        + tmpDir.getAbsolutePath());
                plot.addY((System.nanoTime() - startTime) / 1000000000.0);
            }
        }
        plot.finish();
        plot.show();
        //        System.exit(0);
    }
}
