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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.imru.example.utils.CreateHar;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.imruVsSpark.data.DataGenerator;
import exp.imruVsSpark.kmeans.FilledVectors;
import exp.imruVsSpark.kmeans.SKMeansModel;
import exp.imruVsSpark.kmeans.SparseVector;

public class SparkKMeans {
    public static int run(String host, int dataSize, String sparkPath,
            String dataPath, int nodeCount, final int k, int iterations)
            throws Exception {
        {
            File templateDir = new File(DataGenerator.TEMPLATE);
            final DataGenerator dataGenerator = new DataGenerator(dataSize
                    * nodeCount, templateDir);
            final SKMeansModel model = new SKMeansModel(k, dataGenerator, 20);
            byte[] bs = JavaSerializationUtils.serialize(model);
            Rt.p("Model size: %,d", bs.length);
            int frameSize = (int) ((bs.length / 1024 / 1024) * 1.1 + 2);
            System.setProperty("spark.akka.frameSize", Integer
                    .toString(frameSize));
        }
        System.setProperty("spark.broadcast.serverSocketTimeout", "30000");
        //cd /data/b/soft/lib/spark-0.7.0;sbt/sbt package;cp core/target/scala-2.9.2/spark-core_2.9.2-0.7.0.jar /data/a/imru/ucscImru/lib/spark-0.7.0/
        //cd /data/b/soft;lib/spark-0.7.0/run spark.deploy.master.Master -i 192.168.56.101 -p 7077
        //cd /data/b/soft;lib/spark-0.7.0/run spark.deploy.worker.Worker spark://192.168.56.101:7077
        //cd /data/b/soft/spark-0.8.0-incubating;bin/start-master.sh
        //cd /data/b/soft/spark-0.8.0-incubating;./spark-class org.apache.spark.deploy.worker.Worker spark://192.168.56.101:7077

        System.setProperty("SPARK_LOCAL_IP", host);
        File templateDir = new File(DataGenerator.TEMPLATE);
        final DataGenerator dataGenerator = new DataGenerator(dataSize,
                templateDir);
        final int dimensions = dataGenerator.dims;
        final SKMeansModel model = new SKMeansModel(k, dataGenerator, 20);

        ByteArrayOutputStream memory = new ByteArrayOutputStream();
        ZipOutputStream zip2 = new ZipOutputStream(memory);
        CreateHar.add("", new File("bin"), zip2);
        zip2.finish();
        Rt.write(new File("/tmp/simple-project-1.0.jar"), memory.toByteArray());

        String master = "local";
        master = "spark://ec2-174-129-117-0.compute-1.amazonaws.com:7077";
        master = "spark://" + host + ":7077";
        JavaSparkContext sc = new JavaSparkContext(master, "JavaKMeans",
                sparkPath, new String[] { "/tmp/simple-project-1.0.jar" });

        JavaRDD<String> lines = sc.textFile(dataPath, nodeCount);
        JavaRDD<SparseVector> points = lines.map(
                new Function<String, SparseVector>() {
                    public SparseVector call(String line) {
                        return new SparseVector(line);
                    }
                }).cache();

        //Modify core/src/main/scala/org/apache/spark/api/java/function/FlatMapFunction.scala
        //call to call3 to avoid AbstractMethodError
        FlatMapFunction flatMapFunction1 = new FlatMapFunction<Iterator<SparseVector>, FilledVectors>() {
            public java.lang.Iterable<FilledVectors> call(
                    Iterator<SparseVector> input) throws Exception {
                return call3(input);
            }

            public java.lang.Iterable<FilledVectors> call3(
                    Iterator<SparseVector> input) throws Exception {
                FilledVectors result = new FilledVectors(k, dimensions);
                while (input.hasNext()) {
                    SparseVector dataPoint = input.next();
                    SKMeansModel.Result rs = model.classify(dataPoint);
                    result.centroids[rs.belong].add(dataPoint);
                    result.distanceSum += rs.dis;
                }
                return new AggregatedResult<FilledVectors>(result);
            }
        };
        for (int i = 1; i <= iterations; i++) {
            System.out.println("On iteration " + i);
            JavaRDD<FilledVectors> kmeansModels = points
                    .mapPartitions(flatMapFunction1);
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
            Rt.p("Total examples: " + model.totalExamples);
            //            if (!changed)
            //                break;
        }
        sc.stop();
        return model.totalExamples;
    }

    public static void main(String[] args) throws Exception {
        String host = "192.168.56.101";
        host = "wrvm";
        run(host, 1000000, "/data/b/soft/spark-0.8.0-incubating",
                "/data/b/data/imru/productName.txt", 1, 3, 5);
        System.exit(0);
    }
}
