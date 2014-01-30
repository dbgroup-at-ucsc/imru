package edu.uci.ics.hyracks.imru.api;

import java.io.Serializable;

public class ImruParameters implements Serializable {
    /**
     * Start compressing intermediate results
     * after finished the following iterations.
     */
    public int compressIntermediateResultsAfterNIterations = 10;
}
