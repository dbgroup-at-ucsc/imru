package exp.imruVsSpark.kmeans.exp;

public class RunAll {
    public static void main(String[] args) throws Exception {
        DataPointsPerNode.runExp();
        ModelSize.runExp();
        NumberOfNodes.runExp();
        FanInAndK.runExp();
    }
}
