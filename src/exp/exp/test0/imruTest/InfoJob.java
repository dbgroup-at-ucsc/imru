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

package exp.test0.imruTest;

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

/**
 * Core IMRU application specific code.
 * The dataflow is parse->map->reduce->update
 */
public class InfoJob extends ImruObject<String, String, String> {
    int n = 5;

    public InfoJob(int n) {
        this.n = n;
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
            DataWriter<String> output) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String line = reader.readLine();
        reader.close();
        for (String s : line.split(" ")) {
            System.out.println(ctx.getNodeId() + "-" + ctx.getOperatorName()
                    + ": " + s);
            output.addData(s);
        }
    }

    /**
     * For a list of data objects, return one result
     */
    @Override
    public String map(IMRUContext ctx, Iterator<String> input, String model)
            throws IOException {
        //        if (ctx.getNodeId().startsWith("NC0")) {
        //            Rt.p(ctx.getIterationNumber() + " "
        //                    + ctx.getRecoverIterationNumber());
        //            if (ctx.getRecoverIterationNumber() < 0
        //                    && ctx.getRerunCount() < 1) {
        //                Rt.sleep(500);
        //                throw new Error();
        //            }
        //        }
        //        Rt.sleep(1000);
        String result = "";
        while (input.hasNext()) {
            String word = input.next();
            result += word;
            System.out.println(ctx.getNodeId() + "-" + ctx.getOperatorName()
                    + ": " + word + " -> " + result);
        }
        return result;
    }

    /**
     * Combine multiple results to one result
     */
    @Override
    public String reduce(IMRUContext ctx, Iterator<String> input)
            throws IMRUDataException {
        //        Rt.sleep(2000);
        String combined = new String();
        StringBuilder sb = new StringBuilder();
        combined = "(";
        //        Rt.p(ctx.getOperatorName() + " open");
        while (input.hasNext()) {
            //            Rt.p(ctx.getOperatorName() + " next");
            String result = input.next();
            if (sb.length() > 0)
                sb.append("+");
            sb.append(result);
            combined += result;
        }
        combined += ")_" + ctx.getNodeId();
        IMRUReduceContext reduceContext = (IMRUReduceContext) ctx;
        System.out.println(ctx.getNodeId()
                + "-"
                + ctx.getOperatorName()
                + "-"
                + (reduceContext.isLocalReducer() ? "L" : reduceContext
                        .getReducerLevel()) + ": " + sb + " -> " + combined);
        return combined;
    }

    @Override
    public String update(IMRUContext ctx, Iterator<String> input, String model)
            throws IMRUDataException {
        StringBuilder sb = new StringBuilder();
        sb.append("(" + model + ")");
        while (input.hasNext()) {
            String result = input.next();
            sb.append("+" + result);
            model += result;
        }
        System.out.println(ctx.getNodeId() + "-" + ctx.getOperatorName() + ": "
                + sb + " -> " + model);
        return model;
    }

    @Override
    public boolean shouldTerminate(String model, ImruIterInfo info) {
        n--;
        info.printReport();
        return n <= 0;
    }

    @Override
    public String integrate(String model1, String model2) {
        return model1 + "+" + model2;
    }

    @Override
    public RecoveryAction onJobFailed(List<ImruSplitInfo> completedRanges,
            long dataSize, int optimalNodesForRerun, float rerunTime,
            int optimalNodesForPartiallyRerun, float partiallyRerunTime) {
        Rt.p("job failed");
        for (ImruSplitInfo split : completedRanges)
            Rt.np("completed " + split.path);
        return RecoveryAction.Accept;
    }
}
