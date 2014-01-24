package exp.imruVsSpark.lr.imru;

import java.io.Serializable;

public class ImruLRGradient implements Serializable {
    public double[] w;
    public int correct;
    public int total;
    public int totalExamples=0;

    public ImruLRGradient(int dimensions) {
        w = new double[dimensions];
    }
}