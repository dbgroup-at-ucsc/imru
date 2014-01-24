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

package exp.types;

import java.io.Serializable;
import java.util.Arrays;
import java.util.UUID;

import edu.uci.ics.hyracks.imru.util.Rt;

public class FilledVector implements Serializable {
    public float[] dimValues;
    private float squareSum = 0;
    public int count = 0;

    public FilledVector(int dimensions) {
        dimValues = new float[dimensions];
        squareSum = 0;
    }

    public void set(int dim, float value) {
        squareSum -= dimValues[dim] * dimValues[dim];
        dimValues[dim] = value;
        squareSum += value * value;
    }

    public float dis(SparseVector dp) {
        float sum = squareSum;
        if (sum<0)
            throw new Error();
        for (int i = 0; i < dp.keys.length; i++) {
            int d = dp.keys[i];
            sum -= dimValues[d] * dimValues[d];
            float f = dimValues[d] - dp.values[i];
            sum += f * f;
        }
        return (float) Math.sqrt(sum);
    }

    public void add(SparseVector dp) {
        squareSum=-1;
        for (int i = 0; i < dp.values.length; i++) {
            int key=dp.keys[i];
//            squareSum -= dimValues[key] * dimValues[key];
            dimValues[key] += dp.values[i];
//            squareSum += dimValues[key] * dimValues[key];
        }
        count++;
    }

    public boolean set(FilledVector c) {
        this.squareSum=0;
        boolean modified = false;
        if (c.count > 0) {
            for (int i = 0; i < dimValues.length; i++) {
                float f = c.dimValues[i] / c.count;
                if (Math.abs(dimValues[i] - f) > 1E-10)
                    modified = true;
                dimValues[i] = f;
                squareSum+=f*f;
            }
        } else {
            for (int i = 0; i < dimValues.length; i++) {
                if (Math.abs(dimValues[i]) > 1E-10)
                    modified = true;
                dimValues[i] = 0;
            }
        }
        return modified;
    }

    public void add(FilledVector dp) {
        squareSum=-1;
        for (int i = 0; i < dimValues.length; i++)
            dimValues[i] += dp.dimValues[i];
        count += dp.count;
    }
}
