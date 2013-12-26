package edu.uci.ics.hyracks.imru.api;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class IMRUMapContext extends IMRUContext {
    private String dataPath;

    public IMRUMapContext(IHyracksTaskContext ctx, String operatorName,
            String dataPath,int partition,int totalPartitions) {
        super(ctx, operatorName,partition,totalPartitions);
        this.dataPath = dataPath;
    }

    public String getDataPath() {
        return dataPath;
    }
}
