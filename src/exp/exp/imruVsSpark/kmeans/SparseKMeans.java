package exp.imruVsSpark.kmeans;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import edu.uci.ics.hyracks.imru.util.Rt;
import exp.imruVsSpark.data.DataGenerator;

public class SparseKMeans {
    public static void run() throws Exception {
        File templateDir = new File("exp_data/product_name");
        final DataGenerator dataGenerator = new DataGenerator(
                DataGenerator.DEBUG_DATA_POINTS, templateDir);

        final int k = DataGenerator.DEBUG_K;
        final int dimensions = dataGenerator.dims;
        final SKMeansModel model = new SKMeansModel(k, dataGenerator, 20);

        for (int i = 1; i <= DataGenerator.DEBUG_ITERATIONS; i++) {
            System.out.println("On iteration " + i);
            FilledVectors result = new FilledVectors(k, dimensions);
            BufferedReader br = new BufferedReader(new FileReader(
                    "/data/b/data/imru/productName.txt"));
            long start = System.currentTimeMillis();
            for (String line = br.readLine(); line != null; line = br
                    .readLine()) {
                SparseVector dataPoint = new SparseVector(line);
                SKMeansModel.Result rs = model.classify(dataPoint);
                result.centroids[rs.belong].add(dataPoint);
                result.distanceSum += rs.dis;
            }
            Rt.p(result.distanceSum);
            boolean changed = model.set(result);
            if (!changed)
                break;
        }
    }

    public static void main(String[] args) throws Exception {

    }
}
