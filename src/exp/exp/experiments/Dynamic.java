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
import exp.VirtualBoxExperiments;
import exp.imruVsSpark.LocalCluster;
import exp.imruVsSpark.VirtualBox;
import exp.test0.GnuPlot;

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

    public static void runExp(String folder) throws Exception {
        try {
            //            VirtualBox.remove();
            int nodeCount = 8;
            int memory = 2000;
            int k = 3;
            int iterations = 5;
            int batchStart = 1;
            int batchStep = 3;
            int batchEnd = 1;
            int batchSize = 100000;
            int network = 0;
            String cpu = "0.5";
            int fanIn = 2;

            for (int type = 0; type < 2; type++) {
                //                VirtualBoxExperiments.dynamicDebug=true;
                if (type == 0) {
                    VirtualBoxExperiments.resultFolder = folder
                            + "/resultDelay60s";
                    VirtualBoxExperiments.straggler = 60000;
                } else {
                    VirtualBoxExperiments.resultFolder = folder
                            + "/resultNoDelay";
                    VirtualBoxExperiments.straggler = 0;
                }
                VirtualBoxExperiments.IMRU_ONLY = true;
                for (k = 2; k <= 14; k += 2) {
                    for (int i = 0; i < 3; i++) {
                        VirtualBoxExperiments.dynamicAggr = i < 2;
                        VirtualBoxExperiments.disableSwapping = (i == 1);
                        VirtualBoxExperiments.runExperiment(nodeCount, memory,
                                k, iterations, batchStart, batchStep, batchEnd,
                                batchSize, network, cpu, fanIn);
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
                + KmeansFigs.ITERATIONS
                + "b1s3e1b100000/local2000M0.5coreN0_8nodes_nary_2_d");
        KmeansFigs f = new KmeansFigs(file);
        plot.extra = "set title \"K-means" + " 10^5 points/node*" + f.nodeCount
                + " Iteration=" + f.iterations + "\\n cpu=" + f.core
                + "core/node*" + f.nodeCount + " memory=" + f.memory
                + "MB/node*" + f.nodeCount + " \"";
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
            KmeansFigs.figsDir = dir;
            for (int k = 2; k <= 16; k += 2) {
                if (exps[k] == null)
                    exps[k] = new Exp();
                f = new KmeansFigs(new File(KmeansFigs.figsDir, "k" + k + "i"
                        + KmeansFigs.ITERATIONS
                        + "b1s3e1b100000/local2000M0.5coreN0_8nodes_nary_2"));
                exps[k].org.add(f.get("imruMem1"));
                f = new KmeansFigs(new File(KmeansFigs.figsDir, "k" + k + "i"
                        + KmeansFigs.ITERATIONS
                        + "b1s3e1b100000/local2000M0.5coreN0_8nodes_nary_2_ds"));
                exps[k].fixed.add(f.get("imruMem1"));
                f = new KmeansFigs(new File(KmeansFigs.figsDir, "k" + k + "i"
                        + KmeansFigs.ITERATIONS
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
        VirtualBoxExperiments.experiment = "lr";
        String resultFolder = "result_" + VirtualBoxExperiments.experiment;
        for (int i = 0; i < 10; i++)
            runExp(resultFolder + "/dynamic_aggr/" + 3);
        String folder = resultFolder + "/dynamic_aggr/";
        //                plot("straggler", folder, "/resultDelay60s").show();
        plot("normal", folder, "/resultNoDelay").show();
    }
}
