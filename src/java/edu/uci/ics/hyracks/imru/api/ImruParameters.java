package edu.uci.ics.hyracks.imru.api;

import java.io.Serializable;

/**
 * Parameters sent to operators and connectors
 * 
 * @author Rui Wang
 */
public class ImruParameters implements Serializable {
    /**
     * Start compressing intermediate results
     * after finished the following iterations.
     */
    public int compressIntermediateResultsAfterNIterations = 10;

    public boolean dynamicMapping;

    public boolean useMemoryCache;

    public int dynamicMappersPerNode;

    public boolean dynamicAggr;
    public boolean disableSwapping = false;
    public int maxWaitTimeBeforeSwap = 1000;
    public boolean dynamicDebug;
    public int roundNum;
    public int recoverRoundNum;
    public int rerunNum;;
    public  boolean noDiskCache;
}
