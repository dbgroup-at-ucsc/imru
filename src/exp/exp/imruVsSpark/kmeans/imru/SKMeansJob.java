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

public class SKMeansJob implements IIMRUJob<SKMeansModel, SDataPoint, SKMeansCentroids> {
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
        return 1024*1024;
    }

    /**
     * Parse input data and output tuples
     */
    @Override
    public void parse(IMRUContext ctx, InputStream input, DataWriter<SDataPoint> output) throws IOException {
        try {
            Pattern p = Pattern.compile("[ |\\t]+");
            Pattern p2 = Pattern.compile(":");
            BufferedReader reader = new BufferedReader(new InputStreamReader(input));
            int n=0;
            while (true) {
                String line = reader.readLine();
                if (line == null)
                    break;
                String[] ss = p.split(line);
                SDataPoint dataPoint = new SDataPoint(ss.length);
                for (int i = 0; i < ss.length; i++) {
                    String[] kv = p2.split(ss[i]);
                    dataPoint.keys[i] = Integer.parseInt(kv[0]);
                    dataPoint.values[i] = Integer.parseInt(kv[1]);
                }
                output.addData(dataPoint);
                if (n++ > 100)
                    break;
            }
            reader.close();
        } catch (IOException e) {
            throw new IMRUDataException(e);
        }
    }

    @Override
    public SKMeansCentroids map(IMRUContext ctx, Iterator<SDataPoint> input, SKMeansModel model) throws IOException {
        SKMeansCentroids result = new SKMeansCentroids(k, dimensions);
        while (input.hasNext()) {
            SDataPoint dataPoint = input.next();
            // Classify data points using existing centroids
            double min = Double.MAX_VALUE;
            int belong = -1;
            for (int i = 0; i < k; i++) {
                double dis = model.centroids[i].dis(dataPoint);
                if (dis < min) {
                    min = dis;
                    belong = i;
                }
            }
            result.centroids[belong].add(dataPoint);
            result.distanceSum += min;
        }
        //        System.out.println("map "+model);
        return result;
    }

    /**
     * Combine multiple results to one result
     */
    @Override
    public SKMeansCentroids reduce(IMRUContext ctx, Iterator<SKMeansCentroids> input) throws IMRUDataException {
        SKMeansCentroids combined = new SKMeansCentroids(k, dimensions);
        while (input.hasNext()) {
            SKMeansCentroids result = input.next();
            for (int i = 0; i < k; i++)
                combined.centroids[i].add(result.centroids[i]);
            combined.distanceSum += result.distanceSum;
        }
        return combined;
    }

    /**
     * update the model using combined result
     */
    @Override
    public SKMeansModel update(IMRUContext ctx, Iterator<SKMeansCentroids> input, SKMeansModel model)
            throws IMRUDataException {
        SKMeansCentroids combined = reduce(ctx, input);
        boolean changed = false;
        for (int i = 0; i < k; i++)
            changed = changed || model.centroids[i].set(combined.centroids[i]);
        model.roundsRemaining--;
        if (!changed)
            model.roundsRemaining = 0;
        System.out.println("Total distances: " + combined.distanceSum);
        return model;
    }

    /**
     * Return true to exit loop
     */
    @Override
    public boolean shouldTerminate(SKMeansModel model) {
        return model.roundsRemaining <= 0;
    }
}
