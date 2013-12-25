package edu.uci.ics.hyracks.imru.api;

import java.io.OutputStream;
import java.io.Serializable;
import java.util.Iterator;

import edu.uci.ics.hyracks.imru.data.SerializedFrames;
import edu.uci.ics.hyracks.imru.util.Rt;

public abstract class ImruFrames<Model extends Serializable, Data extends Serializable>
        extends ImruStream<Model, Data> {
    SerializedFrames.Receiver reducer;
    ImruIterInfo reducerInfo;
    SerializedFrames.Receiver update;
    ImruIterInfo updateInfo;
    Model updatedModel;

    @Override
    public void reduceInit(final IMRUReduceContext ctx,
            final OutputStream output) throws IMRUDataException {
        reducerInfo = new ImruIterInfo();
        reducer = new SerializedFrames.Receiver() {
            @Override
            public void process(Iterator<byte[]> input, OutputStream output)
                    throws IMRUDataException {
                reduceFrames(ctx, input, output);
            }
        };
        reducer.open(output);
    }

    @Override
    public void reduceReceive(int srcParition, int offset, int totalSize,
            byte[] bs) throws IMRUDataException {
        reducer.receive(srcParition, offset, totalSize, bs);
    }

    @Override
    public void reduceRecvInformation(int srcParition, ImruIterInfo info)
            throws IMRUDataException {
        reducerInfo.add(info);
    }

    @Override
    public ImruIterInfo reduceClose() throws IMRUDataException {
        reducer.close();
        return reducerInfo;
    }

    /**
     * Combine multiple raw data to one binary data
     */
    abstract public void reduceFrames(IMRUReduceContext ctx,
            Iterator<byte[]> input, OutputStream output)
            throws IMRUDataException;

    @Override
    public void updateInit(final IMRUContext ctx, final Model model)
            throws IMRUDataException {
        updateInfo=new ImruIterInfo();
        update = new SerializedFrames.Receiver() {
            @Override
            public void process(Iterator<byte[]> input)
                    throws IMRUDataException {
                updatedModel = updateFrames(ctx, input, model);
            }
        };
        update.open(null);
    }

    @Override
    public void updateReceive(int srcParition, int offset, int totalSize,
            byte[] bs) throws IMRUDataException {
        update.receive(srcParition, offset, totalSize, bs);
    }

    @Override
    public void updateRecvInformation(int srcParition, ImruIterInfo info)
            throws IMRUDataException {
        updateInfo.add(info);
    }

    @Override
    public ImruIterInfo updateClose() throws IMRUDataException {
        update.close();
        return updateInfo;
    }

    @Override
    public Model getUpdatedModel() throws IMRUDataException {
        return updatedModel;
    }

    /**
     * update the model using combined binary data.
     * Return the same model object or return another object.
     */
    abstract public Model updateFrames(IMRUContext ctx, Iterator<byte[]> input,
            Model model) throws IMRUDataException;
}
