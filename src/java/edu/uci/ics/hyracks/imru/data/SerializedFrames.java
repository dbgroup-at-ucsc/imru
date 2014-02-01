package edu.uci.ics.hyracks.imru.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.Future;

import edu.uci.ics.hyracks.api.comm.FrameHelper;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.imru.api.ASyncIO;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api.IMRUDataException;
import edu.uci.ics.hyracks.imru.api.IMRUReduceContext;
import edu.uci.ics.hyracks.imru.api.ImruIterInfo;
import edu.uci.ics.hyracks.imru.api.ImruStream;
import edu.uci.ics.hyracks.imru.dataflow.IMRUDebugger;
import edu.uci.ics.hyracks.imru.dataflow.IMRUSerialize;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.swap.DynamicCommand;
import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * Split binary data into many data frames
 * and then combined them together.
 * Each frame contains the source partition, target partition
 * and reply partition.Each node has multiple sender
 * and one receiver. Source partition is the sender partition.
 * Target partition and reply partition are receiver partition.
 * 
 * @author Rui Wang
 */
public class SerializedFrames {
    public static final int HEADER = 28;
    public static final int TAIL = 20;
    public static final int SOURCE_OFFSET = 4;
    public static final int TARGET_OFFSET = 8;
    public static final int REPLY_OFFSET = 12;
    public static final int SIZE_OFFSET = 16;
    public static final int POSITION_OFFSET = 20;
    public static final int WRITER_OFFSET = 24;

    public static final int DBG_INFO_FRAME = -1;
    public static final int DYNAMIC_COMMUNICATION_FRAME = -2;

    public static class Buf {
        byte[] data;
        int pos = 0;
        int pieces = 0;
        long totalRawData;
        long startTime;
        long totalTime;
    }

    public static class Receiver {
        public String name;
        public ASyncIO<byte[]> io;
        Future future;
        Hashtable<Integer, Buf> hash = new Hashtable<Integer, Buf>();
        public long totalRecvData = 0;
        public long totalRecvTime = 0;

        public Receiver(String name) {
            this.name = name;
        }

        public void process(Iterator<byte[]> input) throws IMRUDataException {
        }

        public void process(Iterator<byte[]> input, OutputStream output)
                throws IMRUDataException {
        }

        public void open() throws IMRUDataException {
            open(null);
        }

        public void open(final OutputStream output) throws IMRUDataException {
            io = new ASyncIO<byte[]>(1);
            future = IMRUSerialize.threadPool.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Iterator<byte[]> input = io.getInput();
                        if (output != null) {
                            ByteArrayOutputStream out = new ByteArrayOutputStream();
                            process(input, out);
                            byte[] objectData = out.toByteArray();
                            output.write(objectData);
                        } else {
                            process(input);
                        }
                    } catch (IMRUDataException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            if (output != null)
                                output.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }

        public void receiveComplete(int srcPartition, byte[] bs)
                throws IMRUDataException {
            io.add(bs);
        }

        public boolean receive(int srcPartition, int offset, int totalSize,
                byte[] bs) throws IMRUDataException {
            Buf buffer = hash.get(srcPartition);
            if (buffer == null) {
                buffer = new Buf();
                buffer.data = new byte[totalSize];
                buffer.pos = 0;
                buffer.startTime = System.currentTimeMillis();
                hash.put(srcPartition, buffer);
            }
            if (buffer.pos != offset)
                throw new IMRUDataException(buffer.pos + " " + offset);
            if (buffer.data.length != totalSize)
                throw new IMRUDataException();
            System.arraycopy(bs, 0, buffer.data, buffer.pos, Math.min(
                    bs.length, buffer.data.length - buffer.pos));
            buffer.pieces++;
            buffer.pos += bs.length;
            buffer.totalRawData += bs.length; //TODO fix this
            if (buffer.pos >= buffer.data.length) {
                buffer.totalTime = System.currentTimeMillis()
                        - buffer.startTime;
                synchronized (this) {
                    totalRecvData += buffer.totalRawData;
                    totalRecvTime += buffer.totalTime;
                }
                hash.remove(srcPartition);
                receiveComplete(srcPartition, buffer.data);
                return true;
            }
            return false;
        }

        public void close() throws IMRUDataException {
            io.close();
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public int srcPartition;
    public int targetParition;
    public int replyPartition;
    public int offset;
    public int receivedSize;
    public int totalSize;
    public byte[] data;

    public static SerializedFrames nextFrame(int frameSize, ByteBuffer buffer)
            throws HyracksDataException {
        if (buffer == null)
            return null;
        int sourcePartition = buffer.getInt(SOURCE_OFFSET);
        SerializedFrames merge = new SerializedFrames();
        merge.totalSize = buffer.getInt(SIZE_OFFSET);
        merge.offset = buffer.getInt(POSITION_OFFSET);
        merge.srcPartition = sourcePartition;
        merge.targetParition = buffer.getInt(TARGET_OFFSET);
        merge.replyPartition = buffer.getInt(REPLY_OFFSET);
        merge.receivedSize = merge.offset + frameSize - HEADER - TAIL;
        merge.data = new byte[frameSize - HEADER - TAIL];
        System.arraycopy(buffer.array(), HEADER, merge.data, 0,
                merge.data.length);
        return merge;
    }

    public static byte[] deserializeFromChunks(int frameSize,
            LinkedList<ByteBuffer> chunks) throws HyracksDataException {
        int curPosition = 0;
        byte[] bs = null;
        for (ByteBuffer buffer : chunks) {
            int size = buffer.getInt(SIZE_OFFSET);
            int position = buffer.getInt(POSITION_OFFSET);
            if (bs == null)
                bs = new byte[size];
            else if (size != bs.length)
                throw new HyracksDataException();
            if (position != curPosition) {
                Rt.p(size);
                Rt.p(position);
                Rt.p(buffer);
                //                System.exit(0);
                throw new HyracksDataException(position + " " + curPosition);
            }
            int len = Math.min(bs.length - curPosition, frameSize - HEADER
                    - TAIL);
            System.arraycopy(buffer.array(), HEADER, bs, curPosition, len);
            curPosition += len;
            if (curPosition >= bs.length)
                break;
        }
        return bs;
    }

    @Deprecated
    public static SerializedFrames nextFrame(IHyracksTaskContext ctx,
            ByteBuffer buffer, Hashtable<Integer, LinkedList<ByteBuffer>> hash)
            throws HyracksDataException {
        return nextFrame(ctx, buffer, hash, null);
    }

    @Deprecated
    public static SerializedFrames nextFrame(IHyracksTaskContext ctx,
            ByteBuffer buffer, Hashtable<Integer, LinkedList<ByteBuffer>> hash,
            String debugInfo) throws HyracksDataException {
        if (buffer == null)
            return null;
        int frameSize = ctx.getFrameSize();
        LinkedList<ByteBuffer> queue = null;
        ByteBuffer frame = ctx.allocateFrame();
        frame.put(buffer.array(), 0, frameSize);
        int sourcePartition = buffer.getInt(SOURCE_OFFSET);
        queue = hash.get(sourcePartition);
        if (queue == null) {
            queue = new LinkedList<ByteBuffer>();
            hash.put(sourcePartition, queue);
        }
        queue.add(frame);
        int size = buffer.getInt(SIZE_OFFSET);
        int position = buffer.getInt(POSITION_OFFSET);
        //        if (position == 0)
        //            Rt.p(position + "/" + size);
        if (debugInfo != null)
            IMRUDebugger.sendDebugInfo("recv " + debugInfo + " " + position);
        SerializedFrames merge = new SerializedFrames();
        merge.srcPartition = sourcePartition;
        merge.targetParition = buffer.getInt(TARGET_OFFSET);
        merge.replyPartition = buffer.getInt(REPLY_OFFSET);
        merge.receivedSize = position + frameSize - HEADER - TAIL;
        merge.totalSize = size;

        if (position + frameSize - HEADER - TAIL >= size) {
            hash.remove(sourcePartition);
            byte[] bs = deserializeFromChunks(ctx.getFrameSize(), queue);
            //        Rt.p("recv " + bs.length + " " + deserialize(bs));
            merge.data = bs;
        }
        return merge;
    }

    public static void serializeToFrames(IMRUContext ctx, IFrameWriter writer,
            byte[] objectData, int partition, int targetPartition,
            String debugInfo) throws HyracksDataException {
        ByteBuffer frame = ctx.allocateFrame();
        serializeToFrames(ctx, frame, ctx.getFrameSize(), writer, objectData,
                partition, targetPartition, partition, debugInfo,
                targetPartition);
    }

    public static void serializeDbgInfo(IMRUContext ctx, IFrameWriter writer,
            ImruIterInfo info, int partition, int targetPartition, int writerId)
            throws IOException {
        byte[] objectData = JavaSerializationUtils.serialize(info);
        ByteBuffer frame = ctx.allocateFrame();
        serializeToFrames(ctx, frame, ctx.getFrameSize(), writer, objectData,
                partition, targetPartition, DBG_INFO_FRAME, null, writerId);
    }

    public static void serializeSwapCmd(IMRUContext ctx, IFrameWriter writer,
            DynamicCommand cmd, int partition, int targetPartition, int writerId)
            throws IOException {
        byte[] objectData = JavaSerializationUtils.serialize(cmd);
        ByteBuffer frame = ctx.allocateFrame();
        int frames = serializeToFrames(ctx, frame, ctx.getFrameSize(), writer,
                objectData, partition, targetPartition,
                DYNAMIC_COMMUNICATION_FRAME, null, writerId);
        if (frames > 1)
            throw new Error(frames + " " + cmd+" "+objectData.length);
    }

    public static Object deserialize(byte[] bytes) {
        try {
            ObjectInputStream ois = new ObjectInputStream(
                    new ByteArrayInputStream(bytes));
            return ois.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static byte[] serialize(Serializable object) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream ois = new ObjectOutputStream(out);
            ois.writeObject(object);
            return out.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private static void setUpFrame(IMRUContext ctx, ByteBuffer encapsulatedChunk) {
        // Set up the proper tuple structure in the frame:
        // Tuple count
        encapsulatedChunk.position(FrameHelper.getTupleCountOffset(ctx
                .getFrameSize()));
        encapsulatedChunk.putInt(1);
        // Tuple end offset
        encapsulatedChunk.position(FrameHelper.getTupleCountOffset(ctx
                .getFrameSize()) - 4);
        encapsulatedChunk.putInt(FrameHelper.getTupleCountOffset(ctx
                .getFrameSize()) - 4);
        // Field end offset
        encapsulatedChunk.position(0);
        encapsulatedChunk.putInt(FrameHelper.getTupleCountOffset(ctx
                .getFrameSize()) - 4);
        encapsulatedChunk.position(0);
    }

    int[] partitionWriter; //Mapping between partition and writer

    /**
     * @param ctx
     * @param frame
     * @param frameSize
     * @param writer
     * @param objectData
     * @param sourcePartition
     * @param targetPartition
     * @param replyPartition
     *            - use -1 to indicate debug information
     * @param debugInfo
     * @throws HyracksDataException
     */
    public static int serializeToFrames(IMRUContext ctx, ByteBuffer frame,
            int frameSize, IFrameWriter writer, byte[] objectData,
            int sourcePartition, int targetPartition, int replyPartition,
            String debugInfo, int writerId) throws HyracksDataException {
        int position = 0;
        //        Rt.p("send " + objectData.length + " " + deserialize(objectData));
        int frames = 0;
        while (position < objectData.length) {
            if (ctx != null)
                setUpFrame(ctx, frame);
            frame.position(SOURCE_OFFSET);
            frame.putInt(sourcePartition);
            frame.putInt(targetPartition);
            frame.putInt(replyPartition);
            frame.putInt(objectData.length);
            frame.putInt(position);
            frame.putInt(writerId);
            //            Rt.p(position);
            int length = Math.min(objectData.length - position, frameSize
                    - HEADER - TAIL);
            frame.put(objectData, position, length);
            //            frame.position(frameSize - TAIL);
            //            frame.putInt(0); //tuple count
            frame.position(frameSize);
            frame.flip();
            //            if (position == 0)
            //                Rt.p("send 0");
            //                Rt.p(frame);
            if (debugInfo != null)
                IMRUDebugger.sendDebugInfo("flush " + debugInfo + " "
                        + position);
            FrameUtils.flushFrame(frame, writer);
            position += length;
            frames++;
        }
        return frames;
    }
}
