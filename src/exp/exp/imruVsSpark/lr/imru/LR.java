package exp.imruVsSpark.lr.imru;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.regex.Pattern;

import edu.uci.ics.hyracks.imru.util.Rt;
import exp.imruVsSpark.data.DataGenerator;
import exp.types.SparseVector;

public class LR {
    static int N = 10000; // Number of data points
    static int D = 100; // Numer of dimensions
    static int V = 10; // Numer of dimensions which have values
    static float R = 0.7f; // Scaling factor
    static int ITERATIONS = 5;
    static Random rand = new Random(99);
    static File datafile = new File("/tmp/cache/lr.txt");

    public static float nextGaussian(double mean, double variance) {
        // http://www.taygeta.com/random/gaussian.html
        double t = Math.sqrt(variance);
        double x1 = rand.nextDouble();
        double x2 = rand.nextDouble();
        double y1 = Math.sqrt(-2 * Math.log(x1)) * Math.cos(2 * Math.PI * x2);
        y1 *= t;
        return (float) (y1 + mean);
    }

//    public static SparseVector[] generateData() {
//        SparseVector[] ds = new SparseVector[N];
//        for (int i = 0; i < N; i++) {
//            SparseVector point = new SparseVector(LR.V);
//            point.positive = i % 2 == 1;
//            for (int j = 0; j < point.values.length; j++) {
//                point.keys[j] = D == V ? j : rand.nextInt(D);
//                point.values[j] = nextGaussian(0, 1)
//                        + (point.positive ? 1 : -1) * R;
//            }
//            ds[i] = point;
//        }
//        return ds;
//    }

    public static int generateDataFile() throws Exception {
        Rt.p("generating data " + N);
        DataGenerator d = new DataGenerator(N,D,
                new File("exp_data/product_name"));
        d.generate(true, LR.datafile);
        return d.numOfDims;
        //        SparseVector[] ps = generateData();
        //        PrintStream ps2 = new PrintStream(LR.datafile);
        //        for (SparseVector p : ps) {
        //            ps2.print(p.positive ? 1 : -1);
        //            for (int i = 0; i < p.values.length; i++)
        //                ps2.print(" " + p.keys[i] + ":" + p.values[i]);
        //            ps2.println();
        //        }
        //        ps2.close();
    }

    public static double[] generateWeights(int dims) {
        Random rand = new Random(42);
        double[] ws = new double[dims];
        for (int i = 0; i < dims; i++)
            ws[i] = rand.nextDouble() * 2 - 1;
        return ws;
    }

    public static SparseVector[] loadData(File file) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));
        return loadData(br);
    }

    static Pattern pattern = Pattern.compile(":");

    public static SparseVector[] loadData(BufferedReader br) throws IOException {
        Vector<SparseVector> ps = new Vector<SparseVector>();
        for (String line = br.readLine(); line != null; line = br.readLine())
            ps.add(new SparseVector(line));
        br.close();
        return ps.toArray(new SparseVector[ps.size()]);
    }

    public static void verify(SparseVector[] ps, double[] ws) throws Exception {
        int correct = 0;
        for (SparseVector p : ps) {
            float innerProduct = 0;
            for (int j = 0; j < p.keys.length; j++)
                innerProduct += ws[p.keys[j]] * p.values[j];

            if ((innerProduct > 0) == p.positive)
                correct++;
        }
        Rt.p("Correct: " + correct + "/" + ps.length);
    }

    public LR() throws Exception {
        int dims;
//        if (!datafile.exists())
            dims=generateDataFile();
        SparseVector[] ps = loadData(datafile);

        double[] ws = LR.generateWeights(dims);
        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            ImruLRGradient gradient = new ImruLRGradient(dims);
            for (int i = 0; i < ps.length; i++) {
                SparseVector p = ps[i];
                p.addGradient(ws, gradient);
            }
            for (int j = 0; j < dims; j++)
                ws[j] += gradient.w[j];
            Rt.p(gradient.correct);
        }
        verify(ps, ws);
    }

    static void run() throws Exception {
        new LR();
    }

    public static void main(String[] args) throws Exception {
        run();
    }
}
