package exp.test0.lr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.regex.Pattern;

import edu.uci.ics.hyracks.imru.example.utils.Client;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.test0.lr.ImruLR.Job;
import exp.test0.lr.ImruLR.Model;

public class LR {
    static int N = 10000; // Number of data points
    static int D = 100; // Numer of dimensions
    static int V = 10; // Numer of dimensions which have values
    static double R = 0.7; // Scaling factor
    static int ITERATIONS = 5;
    static Random rand = new Random(99);
    static File datafile = new File("/tmp/cache/lr.txt");

    public static double nextGaussian(double mean, double variance) {
        // http://www.taygeta.com/random/gaussian.html
        double t = Math.sqrt(variance);
        double x1 = rand.nextDouble();
        double x2 = rand.nextDouble();
        double y1 = Math.sqrt(-2 * Math.log(x1)) * Math.cos(2 * Math.PI * x2);
        y1 *= t;
        return y1 + mean;
    }

    public static DataPoint[] generateData() {
        DataPoint[] ds = new DataPoint[N];
        for (int i = 0; i < N; i++) {
            DataPoint point = new DataPoint();
            point.y = i % 2 == 0 ? -1 : 1;
            for (int j = 0; j < point.values.length; j++) {
                point.fieldIds[j] = D == V ? j : rand.nextInt(D);
                point.values[j] = nextGaussian(0, 1) + point.y * R;
            }
            ds[i] = point;
        }
        return ds;
    }

    public static void generateDataFile() throws IOException {
        Rt.p("generating data " + N + " " + D);
        DataPoint[] ps = generateData();
        PrintStream ps2 = new PrintStream(LR.datafile);
        for (DataPoint p : ps) {
            ps2.print(p.y);
            for (int i = 0; i < p.values.length; i++)
                ps2.print(" " + p.fieldIds[i] + ":" + p.values[i]);
            ps2.println();
        }
        ps2.close();
    }

    public static double[] generateWeights() {
        Random rand = new Random(42);
        double[] ws = new double[D];
        for (int i = 0; i < D; i++)
            ws[i] = rand.nextDouble() * 2 - 1;
        return ws;
    }

    public static DataPoint[] loadData(File file) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));
        return loadData(br);
    }

    static Pattern pattern = Pattern.compile(":");

    public static DataPoint parseData(String line) {
        StringTokenizer tok = new StringTokenizer(line, " ");
        DataPoint p = new DataPoint();
        p.y = Double.parseDouble(tok.nextToken());
        for (int j = 0; j < V; j++) {
            String s = tok.nextToken();
            String[] ss = pattern.split(s);
            p.fieldIds[j] = Integer.parseInt(ss[0]);
            p.values[j] = Double.parseDouble(ss[1]);
        }
        return p;
    }

    public static DataPoint[] loadData(BufferedReader br) throws IOException {
        Vector<DataPoint> ps = new Vector<DataPoint>();
        for (String line = br.readLine(); line != null; line = br.readLine())
            ps.add(parseData(line));
        br.close();
        return ps.toArray(new DataPoint[ps.size()]);
    }

    public static void verify(DataPoint[] ps, double[] ws) throws Exception {
        int correct = 0;
        for (DataPoint p : ps) {
            float innerProduct = 0;
            for (int j = 0; j < V; j++)
                innerProduct += ws[p.fieldIds[j]] * p.values[j];

            if ((innerProduct > 0) == (p.y > 0))
                correct++;
        }
        Rt.p("Correct: " + correct + "/" + ps.length);
    }

    public LR() throws Exception {
        if (!datafile.exists())
            generateDataFile();
        DataPoint[] ps = loadData(datafile);

        double[] ws = LR.generateWeights();
        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            double[] gradient = new double[D];
            int correct = 0;
            for (int i = 0; i < ps.length; i++) {
                DataPoint p = ps[i];
                if (p.addGradient(ws, gradient))
                    correct++;
            }
            for (int j = 0; j < D; j++)
                ws[j] -= gradient[j];
            Rt.p(correct);
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
