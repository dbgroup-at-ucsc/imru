package edu.uci.ics.hyracks.imru.api;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Vector;

public class ImruIterationInformation<T> implements Serializable {
    public int currentIteration = -1;
    public int finishedRecoveryIteration = 0;
    public long mappedDataSize;
    public int mappedRecords;
    public T object;
    public Vector<String> completedPaths = new Vector<String>();

    public ImruIterationInformation() {
    }

    public void add(ImruIterationInformation r2) {
        if (this.currentIteration < 0)
            this.currentIteration = r2.currentIteration;
        this.mappedDataSize += r2.mappedDataSize;
        this.mappedRecords += r2.mappedRecords;
        HashSet<String> hash = new HashSet<String>();
        for (String s : completedPaths)
            hash.add(s);
        for (Object object : r2.completedPaths) {
            String s = (String) object;
            if (!hash.contains(s)) {
                hash.add(s);
                this.completedPaths.add(s);
            }
        }
    }

    @Override
    public String toString() {
        if (object != null)
            return object.toString();
        return super.toString();
    }
}
