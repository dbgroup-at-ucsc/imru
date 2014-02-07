package edu.uci.ics.hyracks.imru.elastic.wrapper;

import java.io.IOException;
import java.nio.ByteBuffer;

abstract public class ImruWriter {
    public void open() throws IOException {
    }

    abstract public void nextFrame(ByteBuffer buffer) throws IOException;

    public void fail() throws IOException {
    }

    abstract public void close() throws IOException;

    public long getFileSize() {
        return -1;
    }
    
    public String getPath() {
        return null;
    }

    public ImruReader getReader() throws IOException {
        return null;
    }
}
