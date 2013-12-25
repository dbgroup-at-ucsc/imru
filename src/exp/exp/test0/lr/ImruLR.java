package exp.test0.lr;

import java.io.BufferedReader;
import java.io.File;
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
import edu.uci.ics.hyracks.imru.api.old.IIMRUJob;
import edu.uci.ics.hyracks.imru.example.utils.Client;

public class ImruLR {
    static class Model implements Serializable {
        int iterationRemaining = LR.ITERATIONS;
        double[] w = LR.generateWeights();
    }

    static class Job extends ImruObject<Model, DataPoint, double[]> {
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
        public double[] map(IMRUContext ctx, Iterator<DataPoint> input,
                Model model) throws IOException {
            double[] gradient = new double[LR.D];
            while (input.hasNext()) {
                DataPoint data = input.next();
                data.addGradient(model.w, gradient);
            }
            return gradient;
        }

        @Override
        public double[] reduce(IMRUContext ctx, Iterator<double[]> input)
                throws IMRUDataException {
            double[] gradient = new double[LR.D];
            while (input.hasNext()) {
                double[] g = input.next();
                for (int i = 0; i < g.length; i++)
                    gradient[i] -= g[i];
            }
            return gradient;
        }

        @Override
        public Model update(IMRUContext ctx, Iterator<double[]> input,
                Model model) throws IMRUDataException {
            double[] gradient = reduce(ctx, input);
            for (int i = 0; i < gradient.length; i++)
                model.w[i] += gradient[i];
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
        cmdline += "-host localhost -port 3099 -debug";// -disable-logging";
        cmdline += " -example-paths " + LR.datafile.getAbsolutePath();
        Model model = Client.run(new Job(), new Model(), cmdline.split(" "));
        LR.verify(LR.loadData(LR.datafile), model.w);
    }

    public static void main(String[] args) throws Exception {
        run();
        System.exit(0);
    }
}
