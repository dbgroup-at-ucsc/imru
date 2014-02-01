package edu.uci.ics.hyracks.imru.dataflow.dynamic.test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import edu.uci.ics.hyracks.imru.api.DataWriter;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api.IMRUDataException;
import edu.uci.ics.hyracks.imru.api.IMRUMapContext;
import edu.uci.ics.hyracks.imru.api.IMRUReduceContext;
import edu.uci.ics.hyracks.imru.api.ImruIterInfo;
import edu.uci.ics.hyracks.imru.api.ImruObject;
import edu.uci.ics.hyracks.imru.api.ImruOptions;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.AggrStates;
import edu.uci.ics.hyracks.imru.util.Client;
import edu.uci.ics.hyracks.imru.util.Rt;

public class DynamicMappingFunctionalTest {
    static class Job extends ImruObject<String, String, String> {
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
            IMRUMapContext map = (IMRUMapContext) ctx;
            output.addData("" + map.getSplit().uuid);
        }

        /**
         * For a list of data objects, return one result
         */
        @Override
        public String map(IMRUContext ctx, Iterator<String> input, String model)
                throws IOException {
            String result = "";
            while (input.hasNext()) {
                String word = input.next();
                result += word;
                System.out
                        .println(ctx.getNodeId() + "-" + ctx.getOperatorName()
                                + ": " + word + " -> " + result);
            }
            if (ctx.getPartition() == 0)
                Rt.sleep(1000);
            return result;
        }

        /**
         * Combine multiple results to one result
         */
        @Override
        public String reduce(IMRUContext ctx, Iterator<String> input)
                throws IMRUDataException {
            String combined = new String();
            StringBuilder sb = new StringBuilder();
            combined = "(";
            while (input.hasNext()) {
                String result = input.next();
                if (sb.length() > 0)
                    sb.append("+");
                sb.append(result);
                combined += result;
            }
            combined += ")_" + ctx.getNodeId();
            IMRUReduceContext reduceContext = (IMRUReduceContext) ctx;
            System.out
                    .println(ctx.getNodeId()
                            + "-"
                            + ctx.getOperatorName()
                            + "-"
                            + (reduceContext.isLocalReducer() ? "L"
                                    : reduceContext.getReducerLevel()) + ": "
                            + sb + " -> " + combined);
            return combined;
        }

        /**
         * update the model using combined result
         */
        @Override
        public String update(IMRUContext ctx, Iterator<String> input,
                String model) throws IMRUDataException {
            StringBuilder sb = new StringBuilder();
            sb.append("(" + model + ")");
            while (input.hasNext()) {
                String result = input.next();
                sb.append("+" + result);
                model += result;
            }
            System.out.println(ctx.getNodeId() + "-" + ctx.getOperatorName()
                    + ": " + sb + " -> " + model);
            return model;
        }

        @Override
        public boolean shouldTerminate(String model, ImruIterInfo info) {
            return true;
        }
    };

    public static void main(String[] args) {
        try {
            ImruOptions option = new ImruOptions();
            option.host = "localhost";
            option.port = 3099;
            option.debug = true;
            option.disableLogging = true;
            option.numOfNodes = 3;
            option.aggTreeType = "nary";
            option.fanIn = 3;
            //            option.frameSize = 256;
            option.inputPaths = "data/kmeans/kmeans0.txt";
            option.numSplits = 10;
            option.memCache = true; //TODO test without mem cache
            option.dynamicMapping = true;
//            option.dynamicDisableSwapping = true;
            option.dynamicMappersPerNode = 1;
            option.dynamicAggr = true;
//            option.dynamicDebug = true;
            new Thread() {
                public void run() {
                    try {
                        while (true) {
                            if (System.in.available() > 0) {
                                while (System.in.available() > 0)
                                    System.in.read();
                                AggrStates.printAggrTree();
                            }
                            Thread.sleep(500);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                };
            }.start();
            String finalModel = Client.run(new Job(), "", option);
            System.out.println("FinalModel: " + finalModel);
        } catch (Throwable e) {
            Rt.p("failed");
            e.printStackTrace();
            System.exit(0);
        }
        System.exit(0);
    }
}
