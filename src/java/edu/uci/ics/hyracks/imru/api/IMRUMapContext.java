package edu.uci.ics.hyracks.imru.api;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.imru.file.HDFSSplit;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRURuntimeContext;

public class IMRUMapContext extends IMRUContext {
    HDFSSplit split;

    public IMRUMapContext(IHyracksTaskContext ctx, String operatorName,
            HDFSSplit split, int partition, int totalPartitions) {
        super(ctx, operatorName, partition, totalPartitions);
        this.split = split;
    }

    public IMRUMapContext(String nodeId, int frameSize,
            IMRURuntimeContext runtimeContext, String operatorName,
            HDFSSplit split, int partition, int totalPartitions) {
        super(nodeId, frameSize, runtimeContext, operatorName, partition,
                totalPartitions);
        this.split = split;
    }

    public HDFSSplit getSplit() {
        return split;
    }

    public void setSplit(HDFSSplit split) {
        this.split = split;
    }

    public String getDataPath() {
        return split.getPath();
    }
}
