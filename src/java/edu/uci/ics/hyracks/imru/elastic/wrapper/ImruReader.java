package edu.uci.ics.hyracks.imru.elastic.wrapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

abstract public class ImruReader {
    abstract public boolean nextFrame(ByteBuffer buffer) throws IOException;

    public Iterator<ByteBuffer> getIterator(int frameSize) {
        final ByteBuffer inputFrame = ByteBuffer.allocate(frameSize);
        Iterator<ByteBuffer> input = new Iterator<ByteBuffer>() {
            boolean read = false;
            boolean hasData;

            @Override
            public void remove() {
            }

            @Override
            public ByteBuffer next() {
                if (!hasNext())
                    return null;
                read = false;
                return inputFrame;
            }

            @Override
            public boolean hasNext() {
                try {
                    if (!read) {
                        hasData = ImruReader.this.nextFrame(inputFrame);
                        read = true;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return hasData;
            }
        };
        return input;
    }
}
