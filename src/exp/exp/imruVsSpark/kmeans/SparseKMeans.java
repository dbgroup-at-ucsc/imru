package exp.imruVsSpark.kmeans;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.Vector;

import edu.uci.ics.hyracks.imru.util.Rt;
import exp.imruVsSpark.data.DataGenerator;

public class SparseKMeans {
    public static void run(String path) throws Exception {
        int points = 1000000;
        final int k = 3;
        int iteration = 5;
        System.setProperty("spark.akka.frameSize", "16");
        File templateDir = new File(DataGenerator.TEMPLATE);
        final DataGenerator dataGenerator = new DataGenerator(points,
                templateDir);

        final int dimensions = dataGenerator.dims;
        final SKMeansModel model = new SKMeansModel(k, dataGenerator, 20);
        Vector<SparseVector> data = new Vector<SparseVector>(points);

        BufferedReader br = new BufferedReader(
                new InputStreamReader(new BufferedInputStream(
                        new FileInputStream(path), 1024 * 1024)));
        for (String line = br.readLine(); line != null; line = br.readLine())
            data.add(new SparseVector(line));
        br.close();

        for (int i = 1; i <= iteration; i++) {
            System.out.println("On iteration " + i);
            FilledVectors result = new FilledVectors(k, dimensions);

            long start = System.currentTimeMillis();
            int n = 0;
            for (SparseVector dataPoint : data) {
                SKMeansModel.Result rs = model.classify(dataPoint);
                result.centroids[rs.belong].add(dataPoint);
                result.distanceSum += rs.dis;
                n++;
            }
            Rt.p(result.distanceSum + "\t%,d\t%,d", n, System
                    .currentTimeMillis()
                    - start);
            boolean changed = model.set(result);
            if (!changed)
                break;
        }
    }

    public static void main(String[] args) throws Exception {
        run("/data/b/data/imru/productName.txt");
    }
}
