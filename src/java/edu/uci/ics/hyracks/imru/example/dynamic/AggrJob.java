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

package edu.uci.ics.hyracks.imru.example.dynamic;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;

import edu.uci.ics.hyracks.imru.api.DataWriter;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api.IMRUDataException;
import edu.uci.ics.hyracks.imru.api.IMRUReduceContext;
import edu.uci.ics.hyracks.imru.api.ImruIterInfo;
import edu.uci.ics.hyracks.imru.api.ImruObject;
import edu.uci.ics.hyracks.imru.api.ImruSplitInfo;
import edu.uci.ics.hyracks.imru.api.RecoveryAction;
import edu.uci.ics.hyracks.imru.api.old.IIMRUJob;
import edu.uci.ics.hyracks.imru.util.Rt;

public class AggrJob extends ImruObject<byte[], byte[], byte[]> {
    int modelSize = 1024 * 1024;
    int n = 5;

    public AggrJob(int modelSize, int n) {
        this.modelSize = modelSize;
        this.n = n;
    }

    /**
     * Frame size must be large enough to store at least one data object
     */
    @Override
    public int getCachedDataFrameSize() {
        return modelSize * 2;
    }

    /**
     * Parse input data and output data objects
     */
    @Override
    public void parse(IMRUContext ctx, InputStream input,
            DataWriter<byte[]> output) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String line = reader.readLine();
        reader.close();
        for (String s : line.split(" ")) {
            output.addData(new byte[modelSize]);
        }
    }

    /**
     * For a list of data objects, return one result
     */
    @Override
    public byte[] map(IMRUContext ctx, Iterator<byte[]> input, byte[] model)
            throws IOException {
        Rt.sleep(ctx.getPartition() * 500);
        byte[] result = new byte[modelSize];
        while (input.hasNext()) {
            byte[] word = input.next();
            for (int i = 0; i < word.length; i++)
                result[i] += word[i];
        }
        return result;
    }

    /**
     * Combine multiple results to one result
     */
    @Override
    public byte[] reduce(IMRUContext ctx, Iterator<byte[]> input)
            throws IMRUDataException {
        byte[] result = new byte[modelSize];
        while (input.hasNext()) {
            byte[] word = input.next();
            for (int i = 0; i < word.length; i++)
                result[i] += word[i];
        }
        return result;
    }

    @Override
    public byte[] update(IMRUContext ctx, Iterator<byte[]> input, byte[] model)
            throws IMRUDataException {
        byte[] result = new byte[modelSize];
        while (input.hasNext()) {
            byte[] word = input.next();
            for (int i = 0; i < word.length; i++)
                result[i] += word[i];
        }
        return result;
    }

    @Override
    public boolean shouldTerminate(byte[] model, ImruIterInfo info) {
        n--;
        return n <= 0;
    }
}
