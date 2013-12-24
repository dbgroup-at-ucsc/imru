package edu.uci.ics.hyracks.imru.api;

import java.io.OutputStream;
import java.io.Serializable;
import java.util.Iterator;

import edu.uci.ics.hyracks.imru.data.SerializedFrames;

public abstract class ImruFrames<Model extends Serializable, Data extends Serializable>
        extends ImruStream<Model, Data> {
    SerializedFrames.Receiver reducer;
    SerializedFrames.Receiver update;
    Model updatedModel;

    @Override
    public void reduceInit(final IMRUReduceContext ctx,
            final OutputStream output) throws IMRUDataException {
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
    public void reduceClose() throws IMRUDataException {
        reducer.close();
    }

    /**
     * Combine multiple raw data to one binary data
     */
    abstract public void reduceFrames(IMRUReduceContext ctx,
            Iterator<byte[]> input, OutputStream output)
            throws IMRUDataException;

    @Override
    public void updateInit(final IMRUContext ctx, final Model model,
            final ImruIterationInformation runtimeInformation)
            throws IMRUDataException {
        update = new SerializedFrames.Receiver() {
            @Override
            public void process(Iterator<byte[]> input)
                    throws IMRUDataException {
                updatedModel = updateFrames(ctx, input, model,
                        runtimeInformation);
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
    public Model updateClose() throws IMRUDataException {
        update.close();
        return updatedModel;
    }

    /**
     * update the model using combined binary data.
     * Return the same model object or return another object.
     */
    abstract public Model updateFrames(IMRUContext ctx, Iterator<byte[]> input,
            Model model, final ImruIterationInformation runtimeInformation)
            throws IMRUDataException;
}
