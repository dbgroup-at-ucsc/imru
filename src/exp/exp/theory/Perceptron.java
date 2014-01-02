package exp.theory;

import java.io.File;
import java.util.Random;
import java.util.Vector;

import edu.uci.ics.hyracks.imru.data.SerializedFrames;
import edu.uci.ics.hyracks.imru.dataflow.IMRUSerialize;
import edu.uci.ics.hyracks.imru.example.bgd.Data;
import edu.uci.ics.hyracks.imru.example.bgd.Gradient;
import edu.uci.ics.hyracks.imru.example.bgd.Model;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.test0.GnuPlot;

public class Perceptron {
    public static void main(String[] args) throws Exception {
        int dims = 1000000;
        int solidDims = 100;
        int iterations = 15;
        String[] expName = new String[100];
        double[][] ratios = new double[100][iterations];
        double[][] errors = new double[100][iterations];
        int totalExperiments = 0;

        File file = new File("/tmp/cache/bgd_data.txt");
        GnuPlot plot = new GnuPlot("compress", "iteration",
                "misclassified/compress ratio %");
        plot.colored = true;
        plot.scale = false;
        plot.startPointType = 1;
        plot.lineColorGroupSize = 2;
        plot.pointTypes = new int[] { 1, 2 };
        plot.pointSize = 1;
        plot.extra = "set yrange [0:100]";
        Random random = new Random();
        double[] ws = new double[dims];
        for (int i = 0; i < ws.length; i++)
            ws[i] = random.nextDouble();
        Vector<Data> vector = new Vector<Data>();
        //        PrintStream ps = new PrintStream(new BufferedOutputStream(
        //                new FileOutputStream(file), 1024 * 1024));
        for (int examples = 10000; examples <= 30000; examples += 10000) {
            expName[totalExperiments] = examples / 1000 + "K ";
            double[] ratio = ratios[totalExperiments];
            double[] error = errors[totalExperiments];
            totalExperiments++;
            for (int exampleId = 0; exampleId < examples; exampleId++) {
                Data data = new Data();
                data.label = random.nextDouble() < 0.5 ? -1 : 1;
                data.fieldIds = new int[solidDims];
                data.values = new float[solidDims];
                for (int i = 0; i < solidDims; i++) {
                    data.fieldIds[i] = random.nextDouble() < 0.5 ? random
                            .nextInt(dims) : random.nextInt(solidDims * 100);
                    data.values[i] = random.nextFloat();
                }
                vector.add(data);
            }

            Model model = new Model(dims, iterations);
            model.stepSize = 1f;
            while (model.roundsRemaining > 0) {
                //            BufferedReader reader = new BufferedReader(new InputStreamReader(
                //                    new BufferedInputStream(new FileInputStream(file),
                //                            1024 * 1024)));
                Gradient g = new Gradient(dims);
                //            for (String line = reader.readLine(); line != null; line = reader
                //                    .readLine()) {
                for (Data data : vector) {
                    //                String[] ss = line.split(",|\\s+");
                    //                Data data = new Data();
                    //                data.label = Integer.parseInt(ss[0]) > 0 ? 1 : -1;
                    //                data.fieldIds = new int[ss.length - 1];
                    //                data.values = new float[ss.length - 1];
                    //                for (int i = 1; i < ss.length; i++) {
                    //                    String[] kv = ss[i].split("[:=]");
                    //                    data.fieldIds[i - 1] = Integer.parseInt(kv[0]);
                    //                    data.values[i - 1] = Float.parseFloat(kv[1]);
                    //                }
                    float innerProduct = 0;
                    for (int i = 0; i < data.fieldIds.length; i++)
                        innerProduct += data.values[i]
                                * model.weights[data.fieldIds[i]];
                    if ((data.label > 0) != (innerProduct > 0)) {
                        for (int i = 0; i < data.fieldIds.length; i++)
                            g.gradient[data.fieldIds[i]] += data.label
                                    * data.values[i];
                    } else {
                        g.correct++;
                    }
                    g.total++;
                }
                byte[] data = SerializedFrames.serialize(g);
                byte[] compressed = IMRUSerialize.compress(data);
                model.error = 100f * (g.total - g.correct) / g.total;
                for (int i = 0; i < model.weights.length; i++)
                    model.weights[i] += g.gradient[i] / g.total
                            * model.stepSize;
                model.stepSize *= 0.9;

                Rt.p(model.error);
                Rt.p("compression ratio: %.2f%%", compressed.length * 100.0
                        / data.length);
                ratio[model.roundsCompleted] = compressed.length * 100.0
                        / data.length;
                error[model.roundsCompleted] = model.error;

                if (model.error < 0.0001)
                    model.roundsRemaining = 0;
                else
                    model.roundsRemaining--;
                model.roundsCompleted++;
            }
        }
        String[] plotNames = new String[totalExperiments * 2];
        for (int j = 0; j < totalExperiments; j++) {
            plotNames[j * 2] = expName[j] + " compression";
            plotNames[j * 2 + 1] = expName[j] + " misclassification";
        }
        plot.setPlotNames(plotNames);
        for (int i = 0; i < iterations; i++) {
            plot.startNewX(i);
            for (int j = 0; j < totalExperiments; j++) {
                plot.addY(ratios[j][i]);
                plot.addY(errors[j][i]);
            }
        }
        plot.finish();
        plot.show();
        //        BGD.main(args);
    }

}
