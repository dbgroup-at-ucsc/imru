package exp.test0.lr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import edu.uci.ics.hyracks.imru.api.DataWriter;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api.IMRUDataException;
import edu.uci.ics.hyracks.imru.api.ImruIterInfo;
import edu.uci.ics.hyracks.imru.api.ImruObject;
import edu.uci.ics.hyracks.imru.api.ImruSplitInfo;
import edu.uci.ics.hyracks.imru.api.RecoveryAction;
import edu.uci.ics.hyracks.imru.util.Client;
import edu.uci.ics.hyracks.imru.util.Rt;

public class ImruLR {
    static class DataPoint implements Serializable {
        public int[] fieldIds = new int[LR.V];
        public double[] values = new double[LR.V];
        //        double[] x = new double[LR.D];
        double y;

        public boolean addGradient(double[] w, Gradient gradient) {
            boolean correct = false;
            float innerProduct = 0;
            for (int j = 0; j < LR.V; j++)
                innerProduct += w[fieldIds[j]] * values[j];

            if ((innerProduct > 0) == (y > 0))
                correct = true;
            double di = (1 / (1 + Math.exp(-y * innerProduct)) - 1) * y;
            for (int j = 0; j < LR.V; j++)
                gradient.w[fieldIds[j]] -= di * values[j];
            return correct;
        }
    };

    static class Model implements Serializable {
        int iterationRemaining = LR.ITERATIONS;
        double[] w = LR.generateWeights();
    }

    static class Gradient implements Serializable {
        double[] w;
        int correct;
        int total;

        public Gradient(int dimensions) {
            w = new double[dimensions];
        }
    }

    static class Job extends ImruObject<Model, DataPoint, Gradient> {
        @Override
        public int getCachedDataFrameSize() {
            return 1024 * 1024;
        }

        @Override
        public void parse(IMRUContext ctx, InputStream input,
                DataWriter<DataPoint> output) throws IOException {
            DataPoint[] ps = LR.loadData(new BufferedReader(
                    new InputStreamReader(input)));
            for (DataPoint p : ps)
                output.addData(p);
        }

        @Override
        public Gradient map(IMRUContext ctx, Iterator<DataPoint> input,
                Model model) throws IOException {
            Gradient gradient = new Gradient(LR.D);
            while (input.hasNext()) {
                DataPoint data = input.next();
                gradient.total++;
                if (data.addGradient(model.w, gradient))
                    gradient.correct++;
            }
            return gradient;
        }

        @Override
        public Gradient reduce(IMRUContext ctx, Iterator<Gradient> input)
                throws IMRUDataException {
            Gradient gradient = new Gradient(LR.D);
            while (input.hasNext()) {
                Gradient g = input.next();
                for (int i = 0; i < g.w.length; i++)
                    gradient.w[i] += g.w[i];
                gradient.correct += g.correct;
                gradient.total += g.total;
            }
            return gradient;
        }

        @Override
        public Model update(IMRUContext ctx, Iterator<Gradient> input,
                Model model) throws IMRUDataException {
            Gradient gradient = reduce(ctx, input);
            for (int i = 0; i < gradient.w.length; i++)
                model.w[i] += gradient.w[i];
            Rt.p("Correct: " + gradient.correct + "/" + gradient.total);
            model.iterationRemaining--;
            return model;
        }

        @Override
        public boolean shouldTerminate(Model model, ImruIterInfo iterationInfo) {
            return model.iterationRemaining <= 0;
        }

        @Override
        public Model integrate(Model model1, Model model2) {
            return model1;
        }

        @Override
        public RecoveryAction onJobFailed(List<ImruSplitInfo> completedRanges,
                long dataSize, int optimalNodesForRerun, float rerunTime,
                int optimalNodesForPartiallyRerun, float partiallyRerunTime) {
            return RecoveryAction.Accept;
        }
    }

    static void run() throws Exception {
        String cmdline = "";
        cmdline += "-host localhost -port 3099 -debug -disable-logging";
        cmdline += " -input-paths " + LR.datafile.getAbsolutePath();
        if (!LR.datafile.exists())
            LR.generateDataFile();
        Model model = Client.run(new Job(), new Model(), cmdline.split(" "));
        LR.verify(LR.loadData(LR.datafile), model.w);
    }

    public static void main(String[] args) throws Exception {
        run();
        System.exit(0);
    }
}
