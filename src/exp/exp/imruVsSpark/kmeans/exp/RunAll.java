package exp.imruVsSpark.kmeans.exp;

import edu.uci.ics.hyracks.imru.util.Rt;
import exp.imruVsSpark.VirtualBox;

public class RunAll {
    public static void main(String[] args) throws Exception {
        DataPointsPerNode.runExp();
        ModelSize.runExp();
        NumberOfNodes.runExp();
        FanInAndK.runExp();
        VirtualBox.remove();
        Rt.p("Completed");
        System.exit(0);
    }
}
