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

package edu.uci.ics.hyracks.imru.example.kmeans;

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
import edu.uci.ics.hyracks.imru.api.old.IIMRUJob;

/**
 * Random select data examples as centroids
 * 
 * @author wangrui
 */
public class RandomSelectJob extends
        ImruObject<KMeansModel, DataPoint, KMeansStartingPoints> {
    int k;

    public RandomSelectJob(int k) {
        this.k = k;
    }

    /**
     * Frame size must be large enough to store at least one tuple
     */
    @Override
    public int getCachedDataFrameSize() {
        return 256;
    }

    /**
     * Parse input data and output tuples
     */
    @Override
    public void parse(IMRUContext ctx, InputStream input,
            DataWriter<DataPoint> output) throws IOException {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    input));
            while (true) {
                String line = reader.readLine();
                if (line == null)
                    break;
                String[] ss = line.split("[ |\t]+");
                DataPoint dataPoint = new DataPoint();
                dataPoint.x = Double.parseDouble(ss[0]);
                dataPoint.y = Double.parseDouble(ss[1]);
                output.addData(dataPoint);
            }
            reader.close();
        } catch (IOException e) {
            throw new IMRUDataException(e);
        }
    }

    @Override
    public KMeansStartingPoints map(IMRUContext ctx, Iterator<DataPoint> input,
            KMeansModel model) throws IOException {
        KMeansStartingPoints startingPoints = new KMeansStartingPoints(k);
        while (input.hasNext()) {
            DataPoint dataPoint = input.next();
            // random select some data points as
            // starting points
            startingPoints.add(dataPoint);
        }
        return startingPoints;
    }

    /**
     * Combine multiple results to one result
     */
    @Override
    public KMeansStartingPoints reduce(IMRUContext ctx,
            Iterator<KMeansStartingPoints> input) throws IMRUDataException {
        KMeansStartingPoints startingPoints = null;
        while (input.hasNext()) {
            KMeansStartingPoints result = input.next();
            if (startingPoints == null)
                startingPoints = result;
            else
                startingPoints.add(result);

        }
        return startingPoints;
    }

    /**
     * update the model using combined result
     */
    @Override
    public KMeansModel update(IMRUContext ctx,
            Iterator<KMeansStartingPoints> input, KMeansModel model)
            throws IMRUDataException {
        KMeansStartingPoints obj = reduce(ctx, input);
        KMeansStartingPoints startingPoints = (KMeansStartingPoints) obj;
        for (int i = 0; i < k; i++)
            model.centroids[i].set(startingPoints.ps[i]);
        model.roundsRemaining--;
        return model;
    }

    /**
     * Return true to exit loop
     */
    @Override
    public boolean shouldTerminate(KMeansModel model, ImruIterInfo iterationInfo) {
        return model.roundsRemaining <= 0;
    }

    @Override
    public KMeansModel integrate(KMeansModel model1, KMeansModel model2) {
        return model1;
    }

    @Override
    public RecoveryAction onJobFailed(List<ImruSplitInfo> completedRanges,
            long dataSize, int optimalNodesForRerun, float rerunTime,
            int optimalNodesForPartiallyRerun, float partiallyRerunTime) {
        return RecoveryAction.Accept;
    }
}
