package edu.uci.ics.hyracks.imru.api;

import org.kohsuke.args4j.Option;

public class ImruOptions {
    @Option(name = "-debug", usage = "Start cluster controller and node controller in this process for debugging")
    public boolean debug;

    @Option(name = "-debugNodes", usage = "Number of nodes started for debugging")
    public int numOfNodes = 2;

    @Option(name = "-disable-logging", usage = "Disable logging. So console output can be seen when debugging")
    public boolean disableLogging;

    @Option(name = "-host", usage = "Hyracks Cluster Controller Host name", required = true)
    public String host;

    @Option(name = "-port", usage = "Hyracks Cluster Controller Port (default: 1099)")
    public int port = 3099;

    @Option(name = "-clusterport", usage = "Hyracks Cluster Controller Port (default: 3099)")
    public int clusterPort = 1099;

    @Option(name = "-imru-port", usage = "IMRU web service port for uploading and downloading models. (default: 3288)")
    public int imruPort = 3288;

    @Option(name = "-mem-cache", usage = "Load all data into memory")
    public boolean memCache = false;

    @Option(name = "-input-paths", usage = "HDFS path to hold input data. Or local file in the format of [nodeId]:<path>")
    public String inputPaths;

    @Option(name = "-num-split", usage = "Number of splits. Only effective when there is one input file")
    public int numSplits = 1;

    @Option(name = "-min-split-size", usage = "Minimum size of each split")
    public long minSplitSize = 0;

    @Option(name = "-max-split-size", usage = "Maximum size of each split")
    public long maxSplitSize = Long.MAX_VALUE;

    @Option(name = "-splits-per-node", usage = "Do not use. For internal experiment only")
    public int splitsPerNode = 1;

    //For dynamic mapping
    @Option(name = "-dynamicMap", usage = "Dynamically reallocate partitions. Need a proper -num-split to be effective.")
    public boolean dynamicMapping = false;

    @Option(name = "-mappersPerNode", usage = "Limit the number of mappers per node for dynamic mapping")
    public int dynamicMappersPerNode = 1;

    //For dynamic aggregation
    @Option(name = "-dynamic", usage = "Dynamically alter aggregation tree")
    public boolean dynamicAggr = false;

    @Option(name = "-dynamic-disable-swapping", usage = "Use the dynamically dataflow but disable swapping")
    public boolean dynamicDisableSwapping = false;

    @Option(name = "-dynamic-disable-relocation", usage = "Use the dynamically dataflow but disable split relocation")
    public boolean dynamicDisableRelocation = false;

    @Option(name = "-dynamic-swap-time", usage = "Swap nodes after freezed for this time")
    public int dynamicSwapTime = 1000;

    @Option(name = "-dynamic-debug", usage = "Show debugging information of dynamic aggregation")
    public boolean dynamicDebug = false;

    @Option(name = "-no-disk-cache", usage = "Don't cache data on local disk (only works with local data)")
    public boolean noDiskCache = false;

    @Option(name = "-hadoop-conf", usage = "Path to Hadoop configuration")
    public String hadoopConfPath;

    @Option(name = "-cluster-conf", usage = "Path to Hyracks cluster configuration")
    public String clusterConfPath = "conf/cluster.conf";

    @Option(name = "-cc-temp-path", usage = "Path on cluster controller for models")
    public String ccTempPath = "/tmp/imru-cc-models";

    @Option(name = "-nc-temp-path", usage = "Path on each node for cached data")
    public String ncTempPath = "/tmp/imru-nc-models";

    @Option(name = "-save-intermediate-models", usage = "If specified, save intermediate models to this directory.")
    public String localIntermediateModelPath;

    @Option(name = "-model-file-name", usage = "Name of the model file")
    public String modelFileNameHDFS;

    @Option(name = "-agg-tree-type", usage = "The aggregation tree type (none, nary, or generic)")
    public String aggTreeType;

    @Option(name = "-agg-count", usage = "The number of aggregators to use, if using an aggregation tree")
    public int aggCount = -1;

    @Option(name = "-fan-in", usage = "The fan-in, if using an nary aggregation tree")
    public int fanIn = 3;

    @Option(name = "-model-file", usage = "Local file to write the final weights to")
    public String modelFilename;

    @Option(name = "-frame-size", usage = "Hyracks frame size")
    public int frameSize = 0;

    @Option(name = "-compress-after-iterations", usage = "Compress itermediate results after N iterations")
    public int compressAfterNIterations = 10;
}
