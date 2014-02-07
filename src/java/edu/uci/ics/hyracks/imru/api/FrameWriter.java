package edu.uci.ics.hyracks.imru.api;

import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.imru.elastic.wrapper.ImruWriter;

public class FrameWriter {
    IFrameWriter writer;
    ImruWriter w;

    public FrameWriter(IFrameWriter writer) {
        this.writer = writer;
    }

    public FrameWriter(ImruWriter writer) {
        this.w = writer;
    }

    public void writeFrame(ByteBuffer frame) throws HyracksDataException {
        if (writer != null)
            FrameUtils.flushFrame(frame, writer);
        else {
            try {
                w.nextFrame(frame);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
