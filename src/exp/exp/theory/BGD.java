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

public class BGD {
    public static void main(String[] args) throws Exception {
        int dims = 100;
        int solidDims = 10;
        int iterations = 15;
        int examples = 100;

        Random random = new Random();
        double[] ws = new double[dims];
        for (int i = 0; i < ws.length; i++)
            ws[i] = random.nextDouble();
        Vector<Data> vector = new Vector<Data>();
        for (int exampleId = 0; exampleId < examples; exampleId++) {
            Data data = new Data();
            data.label = random.nextDouble() < 0.5 ? 0 : 1;
            data.fieldIds = new int[solidDims];
            data.values = new float[solidDims];
            for (int i = 0; i < solidDims; i++) {
                data.fieldIds[i] = random.nextInt(dims);
                data.values[i] = random.nextFloat();
            }
            vector.add(data);
        }

        Model model = new Model(dims, iterations);
        model.stepSize = 100f;
        while (model.roundsRemaining > 0) {
            Gradient g = new Gradient(dims);
            for (Data data : vector) {
                float innerProduct = 0;
                for (int i = 0; i < data.fieldIds.length; i++)
                    innerProduct += data.values[i]
                            * model.weights[data.fieldIds[i]];
                double p = 1 / (1 + Math.exp(-innerProduct));
                g.total++;
                if ((data.label > 0.5) == (p > 0.5))
                    g.correct++;
                double e = data.label - p;
                for (int i = 0; i < data.fieldIds.length; i++)
                    g.gradient[data.fieldIds[i]] += e * data.values[i];
            }
            byte[] data = SerializedFrames.serialize(g);
            byte[] compressed = IMRUSerialize.compress(data);
            model.error = 100f * (g.total - g.correct) / g.total;
            for (int i = 0; i < model.weights.length; i++)
                model.weights[i] += g.gradient[i] / g.total * model.stepSize;
            model.stepSize *= 0.9;

            Rt.p(model.error);
            Rt.p("compression ratio: %.2f%%", compressed.length * 100.0
                    / data.length);

            if (model.error < 0.0001)
                model.roundsRemaining = 0;
            else
                model.roundsRemaining--;
            model.roundsCompleted++;
        }
    }
}
