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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Random;

import eu.stratosphere.pact.common.type.Value;
import exp.imruVsSpark.data.DataGenerator;
import exp.types.FilledVector;
import exp.types.FilledVectors;
import exp.types.SparseVector;

/**
 * IMRU model which will be used in map() and updated in update()
 */
public class SKMeansModel implements Serializable, Value {
    public FilledVector[] centroids;
    public int k;
    public int dims;
    public int totalExamples = 0;
    public int roundsRemaining = 20;

    public SKMeansModel() {
    }

    public SKMeansModel(int k, DataGenerator dataGenerator, int roundsRemaining) {
        this.roundsRemaining = roundsRemaining;
        centroids = new FilledVector[k];
        this.k = k;
        this.dims = dataGenerator.dims;
        for (int i = 0; i < k; i++) {
            centroids[i] = new FilledVector(dataGenerator.dims);
            centroids[i].count = 1;
            for (int j = 0; j < dataGenerator.dims; j++)
                centroids[i].set(j, dataGenerator.value_distribution.get());
        }
    }

    public SKMeansModel(int k, int dims, float min, float max,
            int roundsRemaining) {
        this.roundsRemaining = roundsRemaining;
        centroids = new FilledVector[k];
        this.k = k;
        this.dims = dims;
        Random random = new Random();
        for (int i = 0; i < k; i++) {
            centroids[i] = new FilledVector(dims);
            centroids[i].count = 1;
            for (int j = 0; j < dims; j++)
                centroids[i].set(j, random.nextFloat() * (max - min) + min);
        }
    }

    public SKMeansModel(FilledVectors combined) {
        this.k = combined.k;
        this.dims = combined.dimensions;
        centroids = new FilledVector[k];
        for (int i = 0; i < k; i++)
            centroids[i] = new FilledVector(dims);
        set(combined);
    }

    public boolean set(FilledVectors combined) {
        boolean changed = false;
        for (int i = 0; i < centroids.length; i++) {
            totalExamples += combined.centroids[i].count;
            if (centroids[i].set(combined.centroids[i]))
                changed = true;
        }
        return changed;
    }

    public static class Result {
        public int belong;
        public float dis;
    }

    public Result classify(SparseVector dataPoint) {
        float min = Float.MAX_VALUE;
        int belong = -1;
        for (int i = 0; i < centroids.length; i++) {
            float dis = centroids[i].dis(dataPoint);
            if (dis < min) {
                min = dis;
                belong = i;
            }
        }
        Result result = new Result();
        result.belong = belong;
        result.dis = min;
        return result;
    }

    @Override
    public String toString() {
        return "" + totalExamples;
    }

    //For stratosphere only
    @Override
    public void read(DataInput dataIn) throws IOException {
        try {
            k = dataIn.readInt();
            dims = dataIn.readInt();
            totalExamples = dataIn.readInt();
            roundsRemaining = dataIn.readInt();
            int size = dataIn.readInt();
            byte[] bs = new byte[size];
            dataIn.readFully(bs);
            ByteArrayInputStream in = new ByteArrayInputStream(bs);
            ObjectInputStream objIn = new ObjectInputStream(in);
            centroids = (FilledVector[]) objIn.readObject();
            objIn.close();
            in.close();
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    //For stratosphere only
    @Override
    public void write(DataOutput out) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(centroids);
        oos.flush();
        out.writeInt(k);
        out.writeInt(dims);
        out.writeInt(totalExamples);
        out.writeInt(roundsRemaining);
        out.writeInt(baos.size());
        out.write(baos.toByteArray());
    }
}
