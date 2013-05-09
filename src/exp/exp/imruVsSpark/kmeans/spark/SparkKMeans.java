package exp.imruVsSpark.kmeans.spark;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.zip.ZipOutputStream;

import scala.Tuple2;
import spark.api.java.JavaPairRDD;
import spark.api.java.JavaRDD;
import spark.api.java.JavaSparkContext;
import spark.api.java.function.FlatMapFunction;
import spark.api.java.function.Function;
import spark.api.java.function.Function2;
import spark.api.java.function.PairFunction;
import edu.uci.ics.hyracks.imru.example.utils.CreateHar;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.imruVsSpark.data.DataGenerator;
import exp.imruVsSpark.kmeans.FilledVectors;
import exp.imruVsSpark.kmeans.SKMeansModel;
import exp.imruVsSpark.kmeans.SparseVector;

public class SparkKMeans {
    public static void run() throws Exception {
        File templateDir = new File("exp_data/product_name");
        final DataGenerator dataGenerator = new DataGenerator(
                DataGenerator.DEBUG_DATA_POINTS, templateDir);

        final int k = DataGenerator.DEBUG_K;
        final int dimensions = dataGenerator.dims;
        final SKMeansModel model = new SKMeansModel(k, dataGenerator, 20);

        ByteArrayOutputStream memory = new ByteArrayOutputStream();
        ZipOutputStream zip2 = new ZipOutputStream(memory);
        CreateHar.add("", new File("bin"), zip2);
        zip2.finish();
        Rt.write(new File("tmp/simple-project-1.0.jar"), memory.toByteArray());

        String master="local";
        master="spark://192.168.56.101:7077";
        JavaSparkContext sc = new JavaSparkContext(master, "JavaLR",
                "lib/spark-0.7.0",
                new String[] { "tmp/simple-project-1.0.jar" });
        JavaRDD<String> lines = sc
                .textFile("/data/b/data/imru/productName.txt");
        JavaRDD<SparseVector> points = lines.map(
                new Function<String, SparseVector>() {
                    public SparseVector call(String line) {
                        return new SparseVector(line);
                    }
                }).cache();

        FlatMapFunction f = new FlatMapFunction<Iterator<SparseVector>, FilledVectors>() {
            public java.lang.Iterable<FilledVectors> call(
                    Iterator<SparseVector> input) throws Exception {
                return call3(input);
            }

            public java.lang.Iterable<FilledVectors> call3(
                    Iterator<SparseVector> input) throws Exception {
                FilledVectors result = new FilledVectors(k, dimensions);
                int n = 0;
                while (input.hasNext()) {
                    SparseVector dataPoint = input.next();
                    SKMeansModel.Result rs = model.classify(dataPoint);
                    result.centroids[rs.belong].add(dataPoint);
                    result.distanceSum += rs.dis;
                }
                return new AggregatedResult<FilledVectors>(result);
            }
        };
        for (int i = 1; i <= DataGenerator.DEBUG_ITERATIONS; i++) {
            System.out.println("On iteration " + i);
            JavaRDD<FilledVectors> kmeansModels = points.mapPartitions(f);
            FilledVectors revisedCentroids = kmeansModels
                    .reduce(new Function2<FilledVectors, FilledVectors, FilledVectors>() {
                        public FilledVectors call(FilledVectors a,
                                FilledVectors b) {
                            FilledVectors result = new FilledVectors(k,
                                    dimensions);
                            result.add(a);
                            result.add(b);
                            return result;
                        }
                    });
            Rt.p(revisedCentroids.distanceSum);
            boolean changed = model.set(revisedCentroids);
            if (!changed)
                break;
        }
        sc.stop();
    }

    public static void main(String[] args) throws Exception {
        run();
        System.exit(0);
    }
}
