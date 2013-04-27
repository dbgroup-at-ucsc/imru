package exp.test0.lr;

import java.io.Serializable;

public class DataPoint implements Serializable{
    public int[] fieldIds=new int[LR.V];
    public double[] values=new double[LR.V];
//    double[] x = new double[LR.D];
    double y;

    public boolean addGradient(double[] w, double[] gradient) {
        boolean correct = false;
        float innerProduct = 0;
        for (int j = 0; j < LR.V; j++)
            innerProduct += w[fieldIds[j]] * values[j];

        if ((innerProduct > 0) == (y > 0))
            correct = true;
        double di = (1 / (1 + Math.exp(-y * innerProduct)) - 1) * y;
        for (int j = 0; j < LR.V; j++)
            gradient[fieldIds[j]] += di * values[j];
        return correct;
    }
}
