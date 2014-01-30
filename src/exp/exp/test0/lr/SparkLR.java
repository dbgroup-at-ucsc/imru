package exp.test0.lr;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.StringTokenizer;
import java.util.Random;
import java.util.zip.ZipOutputStream;

import edu.uci.ics.hyracks.imru.util.Client;
import edu.uci.ics.hyracks.imru.util.CreateDeployment;
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
        CreateDeployment.add("", new File("bin"), zip2);
        zip2.finish();
        Rt.write(new File("tmp/simple-project-1.0.jar"), memory.toByteArray());

        JavaSparkContext sc = new JavaSparkContext("local", "JavaLR",
                "/data/b/soft/spark-0.7.0",
                new String[] { "tmp/simple-project-1.0.jar" });
        JavaRDD<String> lines = sc.textFile(LR.datafile.getAbsolutePath());
        JavaRDD<ImruLR.DataPoint> points = lines.map(
                new Function<String, ImruLR.DataPoint>() {
                    public ImruLR.DataPoint call(String line) {
                        return LR.parseData(line);
                    }
                }).cache();

        final double[] w = LR.generateWeights();

        for (int i = 1; i <= LR.ITERATIONS; i++) {
            System.out.println("On iteration " + i);

            ImruLR.Gradient gradient = points
                    .map(new Function<ImruLR.DataPoint, ImruLR.Gradient>() {
                        public ImruLR.Gradient call(ImruLR.DataPoint p) {
                            ImruLR.Gradient gradient = new ImruLR.Gradient(LR.D);
                            p.addGradient(w, gradient);
                            return gradient;
                        }
                    })
                    .reduce(
                            new Function2<ImruLR.Gradient, ImruLR.Gradient, ImruLR.Gradient>() {
                                public ImruLR.Gradient call(ImruLR.Gradient a,
                                        ImruLR.Gradient b) {
                                    ImruLR.Gradient result = new ImruLR.Gradient(
                                            LR.D);
                                    for (int j = 0; j < LR.D; j++)
                                        result.w[j] = a.w[j] + b.w[j];
                                    return result;
                                }
                            });

            for (int j = 0; j < LR.D; j++)
                w[j] -= gradient.w[j];
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