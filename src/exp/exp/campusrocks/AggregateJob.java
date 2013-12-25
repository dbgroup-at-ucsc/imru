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

package exp.campusrocks;

import java.io.IOException;
import java.io.InputStream;
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
import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * Core IMRU application specific code.
 * The dataflow is parse->map->reduce->update
 */
public class AggregateJob extends ImruObject<byte[], byte[], byte[]> {
    int modelSize;

    public AggregateJob(int modelSize) {
        this.modelSize = modelSize;
    }

    /**
     * Frame size must be large enough to store at least one data object
     */
    @Override
    public int getCachedDataFrameSize() {
        return 256;
    }

    /**
     * Parse input data and output data objects
     */
    @Override
    public void parse(IMRUContext ctx, InputStream input,
            DataWriter<byte[]> output) throws IOException {
        output.addData(new byte[0]);
    }

    /**
     * For a list of data objects, return one result
     */
    @Override
    public byte[] map(IMRUContext ctx, Iterator<byte[]> input, byte[] model)
            throws IOException {
        return new byte[modelSize];
    }

    /**
     * Combine multiple results to one result
     */
    @Override
    public byte[] reduce(IMRUContext ctx, Iterator<byte[]> input)
            throws IMRUDataException {
        while (input.hasNext())
            input.next();
        return new byte[modelSize];
    }

    /**
     * update the model using combined result
     */
    @Override
    public byte[] update(IMRUContext ctx, Iterator<byte[]> input, byte[] model)
            throws IMRUDataException {
        while (input.hasNext())
            input.next();
        return new byte[modelSize];
    }

    /**
     * Return true to exit loop
     */
    @Override
    public boolean shouldTerminate(byte[] model,
            ImruIterInfo iterationInformation) {
        return true;
    }

    @Override
    public byte[] integrate(byte[] model1, byte[] model2) {
        return model1;
    }

    @Override
    public RecoveryAction onJobFailed(List<ImruSplitInfo> completedRanges,
            long dataSize, int optimalNodesForRerun, float rerunTime,
            int optimalNodesForPartiallyRerun, float partiallyRerunTime) {
        return RecoveryAction.Accept;
    }
}
