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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.regex.Pattern;

import edu.uci.ics.hyracks.imru.util.Rt;
import eu.stratosphere.pact.common.type.Value;
import exp.imruVsSpark.lr.imru.ImruLRGradient;

public class SparseVector implements Serializable, Value {
    public boolean positive;
    public int[] keys;
    public float[] values;

    public SparseVector() {
    }

    public SparseVector(String line) {
        Pattern p = Pattern.compile("[ |\\t]+");
        Pattern p2 = Pattern.compile(":");
        String[] ss = p.split(line);
        keys = new int[ss.length - 1];
        values = new float[ss.length - 1];
        if (ss[0].length() > 1) {
            //invalid data due to splitting            
            keys = new int[0];
            values = new float[0];
            return;
        }
        positive = "1".equals(ss[0]);
        for (int i = 1; i < ss.length; i++) {
            String[] kv = p2.split(ss[i]);
            if (kv.length < 2)
                break;
            keys[i - 1] = Integer.parseInt(kv[0]);
            values[i - 1] = Float.parseFloat(kv[1]);
        }
    }

    public SparseVector(int dimensions) {
        keys = new int[dimensions];
        values = new float[dimensions];
    }

    //For stratosphere only
    @Override
    public void read(DataInput in) throws IOException {
        int n = in.readInt();
        keys = new int[n];
        values = new float[n];
        for (int i = 0; i < n; i++) {
            keys[i] = in.readInt();
            values[i] = in.readFloat();
        }
    }

    //For stratosphere only
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(keys.length);
        for (int i = 0; i < keys.length; i++) {
            out.writeInt(keys[i]);
            out.writeFloat(values[i]);
        }
    }

    public void addGradient(double[] w, ImruLRGradient gradient) {
        float innerProduct = 0;
        for (int j = 0; j < keys.length; j++)
            innerProduct += w[keys[j]] * values[j];

        if ((innerProduct > 0) == positive)
            gradient.correct++;
        gradient.total++;
        int y = (positive ? 1 : -1);
        double di = (1 / (1 + Math.exp(-y * innerProduct)) - 1) * y;
        for (int j = 0; j < keys.length; j++)
            gradient.w[keys[j]] -= di * values[j];
        gradient.totalExamples++;
    }
}
