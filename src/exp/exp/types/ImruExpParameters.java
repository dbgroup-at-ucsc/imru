package exp.types;

import java.io.File;

import edu.uci.ics.hyracks.imru.example.utils.Client;
import exp.ElasticImruExperimentEntry;

public class ImruExpParameters {
    public String method; //imruMem, imruDisk, spark, stratosphere
    public String experiment; //kmeans, lr
    public int batchStart;
    public int batchEnd;
    public int batchStep;
    public int batchSize;
    public String master;
    public int nodeCount;
    public int dataSize;
    public String path;
    public boolean memCache;
    public boolean noDiskCache;
    public int k;
    public int iterations;
    public String aggType;
    public int aggArg;
    public boolean dynamic;
    public boolean dynamicDisable;
    public int stragger;
    public File logDir;
    public boolean dynamicDebug;

    public ImruExpParameters() {
    }

    public ImruExpParameters(ElasticImruExperimentEntry.Options options) {
        ImruExpParameters p = this;
        p.method = options.method;
        p.experiment = options.experiment;
        p.batchStart = options.batchStart;
        p.batchEnd = options.batchEnd;
        p.batchStep = options.batchStep;
        p.batchSize = options.batchSize;
        p.master = options.master;
        p.nodeCount = options.nodeCount;
        p.k = options.k;
        p.iterations = options.iterations;
        p.aggType = options.aggTreeType;
        p.aggArg = "generic".equals(options.aggTreeType) ? options.aggCount
                : options.fanIn;
        p.dynamic = options.dynamic;
        p.dynamicDisable = options.dynamicDisable;
        p.stragger = options.straggler;
        p.dynamicDebug = options.dynamicDebug;
    }

    public Client.Options getClientOptions() {
        ImruExpParameters p = this;
        Client.Options options = new Client.Options();
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
        if (p.dynamic)
            options.dynamicAggr = true;
        if (p.dynamicDisable)
            options.dynamicDisable = true;
        if (p.dynamicDebug)
            options.dynamicDebug = true;
        options.inputPaths = p.path;
        return options;
    }
}
