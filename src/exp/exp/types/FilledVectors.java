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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.UUID;

import eu.stratosphere.pact.common.type.Value;

/**
 * Object which is generated by map(), aggregated in reduce() and used in
 * update()
 */
public class FilledVectors implements Serializable, Value {
    String id = UUID.randomUUID().toString();
    public FilledVector[] centroids;
    public int k;
    public int dimensions;
    public double distanceSum = 0;

    public FilledVectors() {
    }

    public FilledVectors(int k, int dimensions) {
        this.k = k;
        this.dimensions = dimensions;
        centroids = new FilledVector[k];
        for (int i = 0; i < k; i++)
            centroids[i] = new FilledVector(dimensions);
    }

    public int count() {
        int count = 0;
        for (int i = 0; i < centroids.length; i++)
            count += centroids[i].count;
        return count;
    }

    public void add(FilledVectors a) {
        for (int i = 0; i < centroids.length; i++)
            centroids[i].add(a.centroids[i]);
        distanceSum += a.distanceSum;
    }

    @Override
    public String toString() {
        return id + " " + count();
    }

    //For stratosphere only
    @Override
    public void read(DataInput dataIn) throws IOException {
        try {
            k = dataIn.readInt();
            dimensions = dataIn.readInt();
            distanceSum = dataIn.readDouble();
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
        out.writeInt(dimensions);
        out.writeDouble(distanceSum);
        out.writeInt(baos.size());
        out.write(baos.toByteArray());
    }
}
