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

package exp.imruVsSpark.kmeans;

import java.io.Serializable;
import java.util.regex.Pattern;

public class SparseVector implements Serializable {
    public int[] keys;
    public float[] values;

    public SparseVector(String line) {
        Pattern p = Pattern.compile("[ |\\t]+");
        Pattern p2 = Pattern.compile(":");
        String[] ss = p.split(line);
        keys = new int[ss.length];
        values = new float[ss.length];
        for (int i = 0; i < ss.length; i++) {
            String[] kv = p2.split(ss[i]);
            keys[i] = Integer.parseInt(kv[0]);
            values[i] = Integer.parseInt(kv[1]);
        }
    }

    public SparseVector(int dimensions) {
        keys = new int[dimensions];
        values = new float[dimensions];
    }
}
