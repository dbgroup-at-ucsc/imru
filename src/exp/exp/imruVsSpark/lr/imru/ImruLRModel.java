package exp.imruVsSpark.lr.imru;

import java.io.Serializable;

public class ImruLRModel implements Serializable {
    int iterationRemaining;
    double[] w;
    int totalExamples = 0;

    public ImruLRModel(int dimensions, int iterations) {
        w = LR.generateWeights(dimensions);
        iterationRemaining = iterations;
    }
}
