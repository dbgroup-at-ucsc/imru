package exp.test0.lr;

import spark.api.java.JavaRDD;
import spark.api.java.JavaSparkContext;
import spark.api.java.function.Function;
import spark.api.java.function.Function2;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.StringTokenizer;
import java.util.Random;
import java.util.zip.ZipOutputStream;

import edu.uci.ics.hyracks.imru.example.utils.Client;
import edu.uci.ics.hyracks.imru.example.utils.CreateHar;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.test0.lr.ImruLR.Job;
import exp.test0.lr.ImruLR.Model;

/**
 * Logistic regression based classification.
 */
public class SparkLR {
    static void run() throws Exception {
        ByteArrayOutputStream memory = new ByteArrayOutputStream();
        ZipOutputStream zip2 = new ZipOutputStream(memory);
        CreateHar.add("", new File("bin"), zip2);
        zip2.finish();
        Rt.write(new File("tmp/simple-project-1.0.jar"), memory.toByteArray());

        JavaSparkContext sc = new JavaSparkContext("local", "JavaLR", "lib/spark-0.7.0",
                new String[] { "tmp/simple-project-1.0.jar" });
        JavaRDD<String> lines = sc.textFile(LR.datafile.getAbsolutePath());
        JavaRDD<DataPoint> points = lines.map(new Function<String, DataPoint>() {
            public DataPoint call(String line) {
                return LR.parseData(line);
            }
        }).cache();

        final double[] w = LR.generateWeights();

        for (int i = 1; i <= LR.ITERATIONS; i++) {
            System.out.println("On iteration " + i);

            double[] gradient = points.map(new Function<DataPoint, double[]>() {
                public double[] call(DataPoint p) {
                    double[] gradient = new double[LR.D];
                    p.addGradient(w, gradient);
                    return gradient;
                }
            }).reduce(new Function2<double[], double[], double[]>() {
                public double[] call(double[] a, double[] b) {
                    double[] result = new double[LR.D];
                    for (int j = 0; j < LR.D; j++)
                        result[j] = a[j] + b[j];
                    return result;
                }
            });

            for (int j = 0; j < LR.D; j++)
                w[j] -= gradient[j];
        }
        sc.stop();
        LR.verify(LR.loadData(LR.datafile), w);
    }

    public static void main(String[] args) throws Exception {
        LR.generateDataFile();
        run();
        System.exit(0);
    }
}