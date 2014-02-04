package edu.uci.ics.hyracks.imru.api;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRURuntimeContext;

public class IMRUReduceContext extends IMRUContext {
    private int level;
    private boolean isLocal;

    public IMRUReduceContext(IHyracksTaskContext ctx, String operatorName,
            boolean isLocal, int level, int partition, int totalPartitions) {
        super(ctx, operatorName, partition, totalPartitions);
        this.isLocal = isLocal;
        this.level = level;
    }

    public IMRUReduceContext(String nodeId, int frameSize,
            IMRURuntimeContext runtimeContext, String operatorName,
            boolean isLocal, int level, int partition, int totalPartitions) {
        super(nodeId, frameSize, runtimeContext, operatorName, partition,
                totalPartitions);
        this.isLocal = isLocal;
        this.level = level;
    }

    public boolean isLocalReducer() {
        return isLocal;
    }

    public int getReducerLevel() {
        return level;
    }
}
