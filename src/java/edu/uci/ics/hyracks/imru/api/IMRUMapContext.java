package edu.uci.ics.hyracks.imru.api;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.imru.file.HDFSSplit;

public class IMRUMapContext extends IMRUContext {
    HDFSSplit split;

    public IMRUMapContext(IHyracksTaskContext ctx, String operatorName,
            HDFSSplit split, int partition, int totalPartitions) {
        super(ctx, operatorName, partition, totalPartitions);
        this.split = split;
    }

    public HDFSSplit getSplit() {
        return split;
    }

    public String getDataPath() {
        return split.getPath();
    }
}
