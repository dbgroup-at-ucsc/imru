package exp.imruVsSpark.kmeans.stratosphere;

import java.io.File;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.imru.util.CreateDeployment;
import edu.uci.ics.hyracks.imru.util.Rt;
import eu.stratosphere.pact.client.PlanExecutor;
import eu.stratosphere.pact.client.RemoteExecutor;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.io.DelimitedOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.CrossStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.generic.contract.BulkIteration;
import exp.imruVsSpark.data.DataGenerator;
import exp.imruVsSpark.kmeans.FilledVectors;
import exp.imruVsSpark.kmeans.SKMeansModel;
import exp.imruVsSpark.kmeans.SparseVector;

/**
 */
public class StratosphereKMeans implements PlanAssembler,
        PlanAssemblerDescription, Serializable {
    public static boolean debug = false;

    static class Classify extends CrossStub implements Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public void cross(PactRecord dataPointRecord, PactRecord models,
                Collector<PactRecord> out) {
            SparseVector dataPoint = dataPointRecord.getField(1,
                    SparseVector.class);
            SKMeansModel model = models.getField(1, SKMeansModel.class);
            SKMeansModel.Result rs = model.classify(dataPoint);
            if (debug)
                Rt.p("classify " + dataPoint.values[0]);
            dataPointRecord.setField(2, new PactInteger(rs.belong));
            dataPointRecord.setField(3, new PactDouble(rs.dis));
            dataPointRecord.setField(4, new PactInteger(model.k));
            dataPointRecord.setField(5, new PactInteger(model.dims));
            out.collect(dataPointRecord);
        }
    }

    static class ComputeCentroids extends ReduceStub implements Serializable {
        private static final long serialVersionUID = 1L;

        private final PactRecord result = new PactRecord(1);

        @Override
        public void reduce(Iterator<PactRecord> pointsWithDistance,
                Collector<PactRecord> out) {
            FilledVectors result = null;

            while (pointsWithDistance.hasNext()) {
                PactRecord res = pointsWithDistance.next();
                SparseVector dataPoint = res.getField(1, SparseVector.class);
                if (debug)
                    Rt.p("compute centroid " + dataPoint.values[0]);
                int belong = res.getField(2, PactInteger.class).getValue();
                double distance = res.getField(3, PactDouble.class).getValue();
                int k = res.getField(4, PactInteger.class).getValue();
                int dims = res.getField(5, PactInteger.class).getValue();
                if (result == null)
                    result = new FilledVectors(k, dims);
                result.centroids[belong].add(dataPoint);
                result.distanceSum += distance;
            }

            this.result.setField(0, new PactInteger(0));
            this.result.setField(1, result);
            out.collect(this.result);
        }
    }

    static class CombineCentroids extends ReduceStub implements Serializable {
        private static final long serialVersionUID = 1L;
        private final PactRecord result = new PactRecord(1);

        @Override
        public void reduce(Iterator<PactRecord> pointsWithDistance,
                Collector<PactRecord> out) {
            FilledVectors result = null;

            if (debug)
                Rt.p("combine centroid");

            while (pointsWithDistance.hasNext()) {
                PactRecord res = pointsWithDistance.next();
                FilledVectors centroids = res.getField(1, FilledVectors.class);
                if (result == null)
                    result = new FilledVectors(centroids.k,
                            centroids.dimensions);
                result.add(centroids);
            }

            SKMeansModel model = new SKMeansModel(result);
            this.result.setField(0, new PactInteger(0));
            this.result.setField(1, model);
            out.collect(this.result);
        }
    }

    @Override
    public Plan getPlan(String... args) {
        final int numOfNodes = Integer.parseInt(args[0]);
        final String dataPointInput = args[1];
        final String clusterInput = args[2];
        final String output = args[3];
        final int numIterations = Integer.parseInt(args[4]);
        final int k = Integer.parseInt(args[5]);
        final int dims = Integer.parseInt(args[6]);

        FileDataSource initialClusterPoints = new FileDataSource(
                new DelimitedInputFormat() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public boolean readRecord(PactRecord record, byte[] line,
                            int offset, int numBytes) {
                        SKMeansModel model = new SKMeansModel(k, dims, 0, 10,
                                20);
                        record.setField(0, new PactInteger(0));
                        record.setField(1, model);
                        return true;
                    }
                }, clusterInput, "Kmeans-model");
        initialClusterPoints.setDegreeOfParallelism(numOfNodes);

        BulkIteration iteration = new BulkIteration("K-Means Loop");
        iteration.setInput(initialClusterPoints);
        iteration.setMaximumNumberOfIterations(numIterations);

        FileDataSource dataPoints = new FileDataSource(
                new DelimitedInputFormat() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public boolean readRecord(PactRecord record, byte[] line,
                            int offset, int numBytes) {
                        String text = new String(line, offset, numBytes);
                        if (debug)
                            Rt.p("read " + text);
                        SparseVector s = new SparseVector(text);
                        record.setField(0, new PactInteger(new Random()
                                .nextInt(numOfNodes)));
                        record.setField(1, s);
                        return true;
                    }
                }, dataPointInput, "Data Points");

        CrossContract computeDistance = CrossContract.builder(new Classify())
                .input1(dataPoints).input2(iteration.getPartialSolution())
                .name("Compute Distances").build();

        ReduceContract computeCentroids = ReduceContract.builder(
                new ComputeCentroids(), PactInteger.class, 0).input(
                computeDistance).name("Compute centroids").build();

        ReduceContract combineCentroids = ReduceContract.builder(
                new CombineCentroids(), PactInteger.class, 0).input(
                computeCentroids).name("Combine Centroids").build();
        iteration.setNextPartialSolution(combineCentroids);

        FileDataSink finalResult = new FileDataSink(
                new DelimitedOutputFormat() {
                    private static final long serialVersionUID = 1L;

                    private final StringBuilder line = new StringBuilder();

                    @Override
                    public int serializeRecord(PactRecord record, byte[] target) {
                        if (debug)
                            Rt.p("output");
                        line.setLength(0);
                        SKMeansModel model = record.getField(1,
                                SKMeansModel.class);
                        line.append(model.totalExamples);
                        //                        for (int i=0;i< model.k;i++) {
                        //                            line.append(" "+model.centroids[i].dimValues[0]);
                        //                        }
                        byte[] byteString = line.toString().getBytes();
                        if (byteString.length <= target.length) {
                            System.arraycopy(byteString, 0, target, 0,
                                    byteString.length);
                            return byteString.length;
                        } else {
                            return -byteString.length;
                        }
                    }
                }, output, iteration, "New Center Positions");
        Plan plan = new Plan(finalResult, "Iterative KMeans");
        plan.setDefaultParallelism(numOfNodes);
        return plan;
    }

    @Override
    public String getDescription() {
        return "Parameters: <numSubStasks> <dataPoints> <clusterCenters> <output> <numIterations>";
    }

    public static int run(String host, int dataSize, String sparkPath,
            String dataPath, int nodeCount, final int k, int iterations)
            throws Exception {
        File templateDir = new File(DataGenerator.TEMPLATE);
        final DataGenerator dataGenerator = new DataGenerator(dataSize
                * nodeCount, templateDir);

        File tmpFile = new File("/tmp/stratosphere_tmp.txt");
        File resultFile = new File("/tmp/stratosphere_result.txt");
        Rt.write(tmpFile, "abc".getBytes());
        StratosphereKMeans kmeans = new StratosphereKMeans();
        Plan plan = kmeans.getPlan("" + nodeCount, //
                "file://" + new File(dataPath).getAbsolutePath(),//
                "file://" + tmpFile.getAbsolutePath(),//
                "file://" + resultFile.getAbsolutePath(),//
                "" + iterations, "" + dataGenerator.dims, "" + k);
        File tmpJar = new File("/tmp/stratosphere_kmeans.jar");
        CreateDeployment.createJar(new File("bin"), tmpJar);
        PlanExecutor ex = new RemoteExecutor(host, 6123, tmpJar
                .getAbsolutePath());
        ex.executePlan(plan);
        return Integer.parseInt(Rt.readFile(resultFile).trim());
    }

    public static void main(String[] args) throws Exception {
        //        debug = true;
        StratosphereKMeans kmeans = new StratosphereKMeans();
        int nodes = 1;
        File tmpFile = new File("tmp/tmp.txt");
        Rt.write(tmpFile, "abc".getBytes());
        Plan plan = kmeans.getPlan("" + nodes, //
                "file://" + new File("exp_data/kmeans.txt").getAbsolutePath(),//
                "file://" + tmpFile.getAbsolutePath(),//
                "file://" + new File("tmp/result.txt").getAbsolutePath(),//
                "5", "2", "2");
        File tmpJar = new File("tmp/stratosphere_kmeans.jar");
        CreateDeployment.createJar(new File("bin"), tmpJar);
        PlanExecutor ex = new RemoteExecutor("localhost", 6123, tmpJar
                .getAbsolutePath());
        ex.executePlan(plan);
    }
}
