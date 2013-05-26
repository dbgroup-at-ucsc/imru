/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package exp.imruVsSpark.kmeans.imru;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.regex.Pattern;

import edu.uci.ics.hyracks.imru.api.DataWriter;
import edu.uci.ics.hyracks.imru.api.IIMRUJob;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api.IMRUDataException;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.imruVsSpark.kmeans.FilledVectors;
import exp.imruVsSpark.kmeans.SKMeansModel;
import exp.imruVsSpark.kmeans.SparseVector;

public class SKMeansJob implements
        IIMRUJob<SKMeansModel, SparseVector, FilledVectors> {
    int k;
    int dimensions;

    public SKMeansJob(int k, int dimensions) {
        this.k = k;
        this.dimensions = dimensions;
    }

    /**
     * Frame size must be large enough to store at least one tuple
     */
    @Override
    public int getCachedDataFrameSize() {
        return 1024 * 1024;
    }

    /**
     * Parse input data and output tuples
     */
    @Override
    public void parse(IMRUContext ctx, InputStream input,
            DataWriter<SparseVector> output) throws IOException {
        try {
            Rt.p("%,d", input.available());
            Pattern p = Pattern.compile("[ |\\t]+");
            Pattern p2 = Pattern.compile(":");
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    input));
            while (true) {
                String line = reader.readLine();
                if (line == null)
                    break;
                String[] ss = p.split(line);
                SparseVector dataPoint = new SparseVector(ss.length);
                for (int i = 0; i < ss.length; i++) {
                    String[] kv = p2.split(ss[i]);
                    dataPoint.keys[i] = Integer.parseInt(kv[0]);
                    dataPoint.values[i] = Integer.parseInt(kv[1]);
                }
                output.addData(dataPoint);
            }
            reader.close();
        } catch (IOException e) {
            throw new IMRUDataException(e);
        }
    }

    @Override
    public FilledVectors map(IMRUContext ctx, Iterator<SparseVector> input,
            SKMeansModel model) throws IOException {
        FilledVectors result = new FilledVectors(k, dimensions);
        while (input.hasNext()) {
            SparseVector dataPoint = input.next();
            SKMeansModel.Result rs = model.classify(dataPoint);
            result.centroids[rs.belong].add(dataPoint);
            result.distanceSum += rs.dis;
        }
//        Rt.p(result.count());
        return result;
    }

    /**
     * Combine multiple results to one result
     */
    @Override
    public FilledVectors reduce(IMRUContext ctx, Iterator<FilledVectors> input)
            throws IMRUDataException {
        FilledVectors combined = new FilledVectors(k, dimensions);
        while (input.hasNext()) {
            FilledVectors result=input.next();
//            Rt.p(result.count());
            combined.add(result);
        }
        return combined;
    }

    /**
     * update the model using combined result
     */
    @Override
    public SKMeansModel update(IMRUContext ctx, Iterator<FilledVectors> input,
            SKMeansModel model) throws IMRUDataException {
        FilledVectors combined = reduce(ctx, input);
        boolean changed = model.set(combined);
//        Rt.p(model.totalExamples);
        model.roundsRemaining--;
        if (!changed)
            model.roundsRemaining = 0;
        System.out.println("Total distances: " + combined.distanceSum
                + " remaining=" + model.roundsRemaining);
        return model;
    }

    /**
     * Return true to exit loop
     */
    @Override
    public boolean shouldTerminate(SKMeansModel model) {
//        Rt.p(model.totalExamples);
        return model.roundsRemaining <= 0;
    }
}
