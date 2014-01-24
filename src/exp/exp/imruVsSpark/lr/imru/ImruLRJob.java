package exp.imruVsSpark.lr.imru;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;

import edu.uci.ics.hyracks.imru.api.DataWriter;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api.IMRUDataException;
import edu.uci.ics.hyracks.imru.api.ImruIterInfo;
import edu.uci.ics.hyracks.imru.api.ImruObject;
import edu.uci.ics.hyracks.imru.api.ImruSplitInfo;
import edu.uci.ics.hyracks.imru.api.RecoveryAction;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.types.SparseVector;

public class ImruLRJob extends
        ImruObject<ImruLRModel, SparseVector, ImruLRGradient> {
    int dimensions;

    public ImruLRJob(int dimensions) {
        this.dimensions = dimensions;
    }

    @Override
    public int getCachedDataFrameSize() {
        return 1024 * 1024;
    }

    @Override
    public void parse(IMRUContext ctx, InputStream input,
            DataWriter<SparseVector> output) throws IOException {
        SparseVector[] ps = LR.loadData(new BufferedReader(
                new InputStreamReader(input)));
        for (SparseVector p : ps)
            output.addData(p);
    }

    @Override
    public ImruLRGradient map(IMRUContext ctx, Iterator<SparseVector> input,
            ImruLRModel model) throws IOException {
        ImruLRGradient gradient = new ImruLRGradient(dimensions);
        while (input.hasNext()) {
            SparseVector data = input.next();
            data.addGradient(model.w, gradient);
        }
        return gradient;
    }

    @Override
    public ImruLRGradient reduce(IMRUContext ctx, Iterator<ImruLRGradient> input)
            throws IMRUDataException {
        ImruLRGradient gradient = new ImruLRGradient(dimensions);
        while (input.hasNext()) {
            ImruLRGradient g = input.next();
            for (int i = 0; i < g.w.length; i++)
                gradient.w[i] += g.w[i];
            gradient.correct += g.correct;
            gradient.total += g.total;
            gradient.totalExamples += g.totalExamples;
        }
        return gradient;
    }

    @Override
    public ImruLRModel update(IMRUContext ctx, Iterator<ImruLRGradient> input,
            ImruLRModel model) throws IMRUDataException {
        ImruLRGradient gradient = reduce(ctx, input);
        for (int i = 0; i < gradient.w.length; i++)
            model.w[i] += gradient.w[i];
        Rt.p("Correct: " + gradient.correct + "/" + gradient.total);
        model.totalExamples += gradient.totalExamples;
        model.iterationRemaining--;
        return model;
    }

    @Override
    public boolean shouldTerminate(ImruLRModel model, ImruIterInfo iterationInfo) {
        return model.iterationRemaining <= 0;
    }
}