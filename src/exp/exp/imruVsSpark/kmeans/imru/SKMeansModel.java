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

import java.io.Serializable;

import exp.imruVsSpark.data.DataGenerator;

/**
 * IMRU model which will be used in map() and updated in update()
 */
public class SKMeansModel implements Serializable {
    SCentroid[] centroids;
    public int roundsRemaining = 20;

    public SKMeansModel(int k, DataGenerator dataGenerator, int roundsRemaining) {
        this.roundsRemaining = roundsRemaining;
        centroids = new SCentroid[k];
        for (int i = 0; i < k; i++) {
            centroids[i] = new SCentroid(dataGenerator.dims);
            centroids[i].count = 1;
            for (int j = 0; j < dataGenerator.dims; j++)
                centroids[i].ds[j] = dataGenerator.value_distribution.get();
        }
    }
}
