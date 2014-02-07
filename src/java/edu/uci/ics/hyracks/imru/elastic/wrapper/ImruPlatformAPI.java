package edu.uci.ics.hyracks.imru.elastic.wrapper;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;

import edu.uci.ics.hyracks.imru.api.IMRUContext;

abstract public class ImruPlatformAPI extends ImruWriter {
    public boolean isHyracks() {
        return true;
    }

    abstract public IMRUContext getContext(String operatorName, int partition,
            int nPartition);

    abstract public ImruWriter createRunFileWriter() throws IOException;

    static Map<Integer, ImruState> stateMap = new Hashtable<Integer, ImruState>();

    public void setIterationState(int partition, ImruState state) {
        stateMap.put(partition, state);
    }

    public ImruState getIterationState(int partition) {
        return stateMap.get(partition);
    }

    public void removeIterationState(int partition) {
        stateMap.remove(partition);
    }
}
