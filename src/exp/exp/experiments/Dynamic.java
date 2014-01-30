package exp.experiments;

import java.io.File;
import java.util.EnumSet;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.ec2.HyracksCluster;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.ImruSendOperator;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.DynamicAggregationStressTest;
import edu.uci.ics.hyracks.imru.util.CreateDeployment;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.ImruExpFigs;
import exp.VirtualBoxExperiments;
import exp.imruVsSpark.LocalCluster;
import exp.imruVsSpark.VirtualBox;
import exp.test0.GnuPlot;
import exp.types.ImruExpParameters;

public class Dynamic {
    public static void disableLogging() throws Exception {
        Logger globalLogger = Logger.getLogger("");
        Handler[] handlers = globalLogger.getHandlers();
        for (Handler handler : handlers)
            globalLogger.removeHandler(handler);
        globalLogger.addHandler(new Handler() {
            @Override
            public void publish(LogRecord record) {
                String s = record.getMessage();
                if (s.contains("Exception caught by thread")) {
                    System.err.println(s);
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() throws SecurityException {
            }
        });
    }

    public static void runExp(String folder, boolean lr) throws Exception {
        try {
            //            VirtualBox.remove();
            ImruExpParameters p = new ImruExpParameters();
            p.experiment="lr";
            p.nodeCount = 8;
            p.memory = 2000;
            p.k = 3;
            p.iterations = 5;
            p.batchStart = 1;
            p.batchStep = 3;
            p.batchEnd = 1;
            p.batchSize = 100000;
            p.network = 0;
            p.cpu = "0.5";
            p.aggArg = 2;
            int startK = 2;
            int endK = 14;
            int startDims = 1000000;
            int stepDims = 3000000;
            int endDims = 10000000;

            if (lr) {
                //for logistic regression
                p.iterations = 1;
                p.batchStart = 10;
                p.batchStep = 10;
                p.batchEnd = 10;
                startK = 0;
                endK = 0;
            }

            for (int type = 0; type < 2; type++) {
                //                VirtualBoxExperiments.dynamicDebug=true;
                if (type == 0) {
                    p.resultFolder = folder + "/resultDelay60s";
                    p.straggler = 60000;
                } else {
                    p.resultFolder = folder + "/resultNoDelay";
                    p.straggler = 0;
                }
                VirtualBoxExperiments.IMRU_ONLY = true;
                for (p.k = startK; p.k <= endK; p.k += 2) {
                    for (p.numOfDimensions = startDims; p.numOfDimensions <= endDims; p.numOfDimensions += stepDims) {
                        for (int i = 0; i < 3; i++) {
                            p.dynamicAggr = i < 2;
                            p.dynamicDisableSwapping = (i == 1);
                            VirtualBoxExperiments.runExperiment(p);
                        }
                    }
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(0);
        } finally {
        }
    }

    static class Value {
        double sum;
        int count = 0;

        public void add(Double d) {
            if (d == null)
                return;
            sum += d;
            count++;
        }

        public Double get() {
            if (count == 0)
                return null;
            return sum / count;
        }
    }

    static class Exp {
        Value org = new Value();
        Value fixed = new Value();
        Value dynamic = new Value();
    }

    public static GnuPlot plot(String name, String prefix, String postfix)
            throws Exception {
        GnuPlot plot = new GnuPlot(new File("/tmp/cache"), name,
                "Model size (MB)", "Time (seconds)");
        File file = new File(new File(prefix + "0" + postfix), "k2i"
                + ImruExpFigs.ITERATIONS
                + "b1s3e1b100000/local2000M0.5coreN0_8nodes_nary_2_d");
        ImruExpFigs f = new ImruExpFigs(file);
        plot.extra = "set title \"K-means" + " 10^5 points/node*"
                + f.p.nodeCount + " Iteration=" + f.p.iterations + "\\n cpu="
                + f.p.cpu + "core/node*" + f.p.nodeCount + " memory="
                + f.p.memory + "MB/node*" + f.p.nodeCount + " \"";
        plot.setPlotNames("Original IMRU", "Fixed aggregation",
                "Dynamic aggregation");
        plot.startPointType = 1;
        plot.pointSize = 1;
        plot.scale = false;
        plot.colored = true;
        Exp[] exps = new Exp[100];
        for (int id = 0;; id++) {
            File dir = new File(prefix + id + postfix);
            if (!dir.exists())
                break;
            ImruExpFigs.figsDir = dir;
            for (int k = 2; k <= 16; k += 2) {
                if (exps[k] == null)
                    exps[k] = new Exp();
                f = new ImruExpFigs(new File(ImruExpFigs.figsDir, "k" + k + "i"
                        + ImruExpFigs.ITERATIONS
                        + "b1s3e1b100000/local2000M0.5coreN0_8nodes_nary_2"));
                exps[k].org.add(f.get("imruMem1"));
                f = new ImruExpFigs(new File(ImruExpFigs.figsDir, "k" + k + "i"
                        + ImruExpFigs.ITERATIONS
                        + "b1s3e1b100000/local2000M0.5coreN0_8nodes_nary_2_ds"));
                exps[k].fixed.add(f.get("imruMem1"));
                f = new ImruExpFigs(new File(ImruExpFigs.figsDir, "k" + k + "i"
                        + ImruExpFigs.ITERATIONS
                        + "b1s3e1b100000/local2000M0.5coreN0_8nodes_nary_2_d"));
                exps[k].dynamic.add(f.get("imruMem1"));
            }
        }
        for (int k = 0; k < exps.length; k++) {
            if (exps[k] == null)
                continue;
            plot.startNewX(k * 5);
            plot.addY(exps[k].org.get());
            plot.addY(exps[k].fixed.get());
            plot.addY(exps[k].dynamic.get());
        }
        plot.finish();
        return plot;
    }

    public static void main(String[] args) throws Exception {
        ImruExpParameters.defExperiment = "lr";
        String resultFolder = "result_" + ImruExpParameters.defExperiment;
        for (int i = 0; i < 10; i++)
            runExp(resultFolder + "/dynamic_aggr/" + i, true);
        String folder = resultFolder + "/dynamic_aggr/";
        //                plot("straggler", folder, "/resultDelay60s").show();
        plot("normal", folder, "/resultNoDelay").show();
    }
}
