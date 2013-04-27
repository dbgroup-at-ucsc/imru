package exp.imruVsSpark.data;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * Return a value based on its distribution
 * 
 * @author Rui Wang
 */
public class Distribution {
    double[] probs;
    private double[] startProbs;
    Random r;

    public Distribution(Random r, File file) throws IOException {
        this.r = r;
        String text = Rt.readFile(file);
        String[] lines = text.split("\n");
        probs = new double[lines.length + 10];
        for (String line : lines) {
            String[] ss = line.split("[ |\t]+");
            probs[Integer.parseInt(ss[0])] = Double.parseDouble(ss[1]);
        }
        calculateStartProbs();
    }

    public Distribution(Random r, double[] probs) {
        this.r = r;
        this.probs = probs;
        calculateStartProbs();
    }

    private void calculateStartProbs() {
        double sum = 0;
        for (double d : probs)
            sum += d;
        for (int i = 0; i < probs.length; i++)
            probs[i] /= sum;
        sum = 0;
        this.startProbs = new double[probs.length];
        for (int i = 0; i < probs.length; i++) {
            startProbs[i] = sum;
            sum += probs[i];
        }
    }

    public int get() {
        return get(r.nextDouble());
    }

    public int get(double p) {
        int t = Arrays.binarySearch(startProbs, p);
        if (t >= 0)
            return t;
        t = -1 - t;
        if (t > 0)
            return t - 1;
        return t;
    }
}
