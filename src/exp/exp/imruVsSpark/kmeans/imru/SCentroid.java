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
import java.util.Arrays;

import edu.uci.ics.hyracks.imru.util.Rt;

public class SCentroid implements Serializable {
    float[] ds;
    int count = 0;

    public SCentroid(int dimensions) {
        ds = new float[dimensions];
    }

    public double dis(SDataPoint dp) {
        float sum = 0;
        int c = 0;
        for (int i = 0; i < dp.keys.length; i++) {
            int d = dp.keys[i];
            while (c < d) {
                sum += ds[c] * ds[c];
                c++;
            }
            float f = ds[d] - dp.values[i];
            c = d + 1;
            sum += f * f;
        }
        return (float) Math.sqrt(sum);
    }

    public void add(SDataPoint dp) {
        for (int i = 0; i < dp.values.length; i++) {
            ds[dp.keys[i]] += dp.values[i];
        }
        count++;
    }

    public boolean set(SCentroid c) {
        boolean modified = false;
        if (c.count > 0) {
            for (int i = 0; i < ds.length; i++) {
                float f = c.ds[i] / c.count;
                if (Math.abs(ds[i] - f) > 1E-10)
                    modified = true;
                ds[i] = f;
            }
        } else {
            for (int i = 0; i < ds.length; i++) {
                if (Math.abs(ds[i]) > 1E-10)
                    modified = true;
                ds[i] = 0;
            }
        }
        return modified;
    }

    public void add(SCentroid dp) {
        for (int i = 0; i < ds.length; i++)
            ds[i] += dp.ds[i];
        count += dp.count;
    }
}
