package exp.test0.lr;

import java.io.File;

import edu.uci.ics.hyracks.ec2.Rt;
import edu.uci.ics.hyracks.imru.example.utils.Client;
import exp.test0.GnuPlot;

public class Test {
    static void fullDimension() throws Exception {
        Client.disableLogging();
        GnuPlot plot = new GnuPlot(new File("result/lr"), "lr", "Dimensions",
                "Time (seconds)");
        plot.extra = "set title \"N=" + LR.N + "\"";
        plot.setPlotNames("Local", "Spark", "IMRU");
        for (LR.D = 10; LR.D <= 1000; LR.D *= 10) {
            LR.V = LR.D;
            LR.generateDataFile();
            long start = System.currentTimeMillis();
            LR.run();
            long localTime = System.currentTimeMillis() - start;
            start = System.currentTimeMillis();
            SparkLR.run();
            long sparkTime = System.currentTimeMillis() - start;
            start = System.currentTimeMillis();
            ImruLR.run();
            long imruTime = System.currentTimeMillis() - start;
            Rt.np("Local: " + localTime);
            Rt.np("Spark: " + sparkTime);
            Rt.np("IMRU: " + imruTime);
            plot.startNewX(LR.D);
            plot.addY(localTime / 1000.0);
            plot.addY(sparkTime / 1000.0);
            plot.addY(imruTime / 1000.0);
        }
        plot.finish();
        System.exit(0);
    }

    static void sparseDimension() throws Exception {
        LR.V = 100;
        Client.disableLogging();
        GnuPlot plot = new GnuPlot(new File("result/lr"), "sparse", "Dimensions",
                "Time (seconds)");
        plot.extra = "set title \"N=" + LR.N + " Non-zero Dimensions=" + LR.V + "\"";
        plot.setPlotNames("Local", "Spark", "IMRU");
        for (LR.D = 1000; LR.D <= 1000000; LR.D *= 10) {
            LR.generateDataFile();
            long start = System.currentTimeMillis();
            LR.run();
            long localTime = System.currentTimeMillis() - start;
            start = System.currentTimeMillis();
            SparkLR.run();
            long sparkTime = System.currentTimeMillis() - start;
            start = System.currentTimeMillis();
            ImruLR.run();
            long imruTime = System.currentTimeMillis() - start;
            Rt.np("Local: " + localTime);
            Rt.np("Spark: " + sparkTime);
            Rt.np("IMRU: " + imruTime);
            plot.startNewX(LR.D);
            plot.addY(localTime / 1000.0);
            plot.addY(sparkTime / 1000.0);
            plot.addY(imruTime / 1000.0);
        }
        plot.finish();
        System.exit(0);
    }

    public static void main(String[] args) throws Exception {
        sparseDimension();
    }
}
