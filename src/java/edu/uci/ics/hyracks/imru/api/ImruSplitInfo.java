package edu.uci.ics.hyracks.imru.api;

import edu.uci.ics.hyracks.imru.file.HDFSSplit;

public class ImruSplitInfo {
    public String path;
    public long start;
    public long end;

    public ImruSplitInfo() {
    }

    public ImruSplitInfo(HDFSSplit split) {
        this.path = split.getPath();
        this.start = split.getStartOffset();
        this.end = split.getStartOffset() + split.getLength();
    }
}
