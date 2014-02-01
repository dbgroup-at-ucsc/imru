package exp.types;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;

import edu.uci.ics.hyracks.imru.api.ImruOptions;
import edu.uci.ics.hyracks.imru.dataflow.IMRUSerialize;
import edu.uci.ics.hyracks.imru.util.Client;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.ElasticImruExperimentEntry;

public class ImruExpParameters implements Serializable {
    public static final long serialVersionUID = 1;
    public static String defExperiment = "lr";
    public String method; //imruMem, imruDisk, spark, stratosphere
    public String experiment=defExperiment; //kmeans, lr
    public int batchStart;
    public int batchEnd;
    public int batchStep;
    public int batchSize;
    public String master;
    public int nodeCount;
    public int dataSize;
    public int numOfDimensions = -1;
    public String path;
    public boolean memCache;
    public boolean noDiskCache;
    public int k;
    public int iterations;
    public String aggType;
    public int aggArg;
    public boolean dynamicAggr;
    public boolean dynamicDisableSwapping;
    public int straggler;
    public File logDir;
    public String resultFolder; //on the developer machine
    public boolean dynamicDebug;

    //for virtualbox experiments
    public int memory; //memory per node
    public int network; //network limit
    public String cpu;
    public boolean monitorMemoryUsage = true;
    public int maxNodesStartupTime = 5 * 60 * 1000;
    public int maxExperimentFreezeTime = 30 * 60 * 1000;

    public ImruExpParameters() {
    }

    public static ImruExpParameters load(File file) throws Exception {
        byte[] bs = Rt.readFileByte(file);
        return (ImruExpParameters) IMRUSerialize.deserialize(bs);
    }

    public byte[] toByteArray() {
        return IMRUSerialize.serialize(this);
    }

    public File getResultFolder() {
        ImruExpParameters p = this;
        String level1 = "k" + p.k + "i" + p.iterations + "b" + p.batchStart
                + "s" + p.batchStep + "e" + p.batchEnd + "b" + p.batchSize
                + "d" + p.numOfDimensions;
        String level2 = "local"
                + p.memory
                + "M"
                + p.cpu
                + "coreN"
                + p.network
                + "_"
                + p.nodeCount
                + "nodes_"
                + p.aggType
                + "_"
                + p.aggArg
                + (p.dynamicAggr ? "_d" + (p.dynamicDisableSwapping ? "s" : "")
                        : "");
        return new File(p.resultFolder + "/" + level1 + "/" + level2);
    }

    public ImruOptions getClientOptions() {
        ImruExpParameters p = this;
        ImruOptions options = new ImruOptions();
        options.host = p.master;
        options.port = 3099;
        //         options.frameSize=16 * 1024 * 1024;
        if (p.aggType == null) {
            options.aggTreeType = "nary";
            options.fanIn = 2;
        } else {
            options.aggTreeType = p.aggType;
            options.aggCount = p.aggArg;
            options.fanIn = p.aggArg;
        }
        options.numSplits = p.nodeCount;

        //            cmdline += "-host localhost -port 3099 -debug -disable-logging";
        if (p.memCache)
            options.memCache = true;
        if (p.noDiskCache)
            options.noDiskCache = true;
        if (p.dynamicAggr)
            options.dynamicAggr = true;
        if (p.dynamicDisableSwapping)
            options.dynamicDisableSwapping = true;
        if (p.dynamicDebug)
            options.dynamicDebug = true;
        options.inputPaths = p.path;
        return options;
    }

    @Override
    public String toString() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(out);
        ps.println("method=" + method);
        ps.println("experiment=" + experiment);
        ps.format("numOfPoints[start=%,d, end=%,d, step=%,d, batch=%,d]\n",
                batchStart, batchEnd, batchStep, batchSize);
        ps.println("master=" + master);
        ps.println("nodeCount=" + nodeCount);
        ps.println("dataSize=" + dataSize);
        ps.println("numOfDimensions=" + numOfDimensions);
        ps.println("path=" + path);
        ps.println("memCache=" + memCache);
        ps.println("noDiskCache=" + noDiskCache);
        ps.println("iterations=" + iterations);
        ps.println("aggType=" + aggType);
        ps.println("aggArg=" + aggArg);
        ps.println("dynamicAggr=" + dynamicAggr);
        ps.println("dynamicDisableSwapping=" + dynamicDisableSwapping);
        ps.println("dynamicDebug=" + dynamicDebug);
        ps.println("stragger=" + straggler);
        ps.println("logDir=" + logDir);
        ps.println("kmeans_k=" + k);
        ps.println("node_memory=" + memory);
        ps.println("node_network=" + network);
        ps.println("node_cpu=" + cpu);
        ps.println("monitorMemoryUsage=" + monitorMemoryUsage);
        ps.println("maxNodesStartupTime=" + maxNodesStartupTime);
        ps.println("maxExperimentFreezeTime=" + maxExperimentFreezeTime);
        ps.println("resultFolder=" + getResultFolder());
        return new String(out.toByteArray());
    }
}
