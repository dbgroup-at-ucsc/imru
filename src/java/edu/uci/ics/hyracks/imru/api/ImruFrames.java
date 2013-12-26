package edu.uci.ics.hyracks.imru.api;

import java.io.OutputStream;
import java.io.Serializable;
import java.util.Iterator;

import edu.uci.ics.hyracks.imru.data.SerializedFrames;
import edu.uci.ics.hyracks.imru.util.Rt;

public abstract class ImruFrames<Model extends Serializable, Data extends Serializable>
        extends ImruStream<Model, Data> {
    public static class A {
        SerializedFrames.Receiver recv;
        ImruIterInfo info;
    }

    Model updatedModel;

    @Override
    public Object reduceInit(final IMRUReduceContext ctx,
            final OutputStream output) throws IMRUDataException {
        A a = new A();
        a.info = new ImruIterInfo(ctx);
        //        Rt.p(reducerInfo.aggrTree.operator + " open");
        a.recv = new SerializedFrames.Receiver() {
            @Override
            public void process(Iterator<byte[]> input, OutputStream output)
                    throws IMRUDataException {
                reduceFrames(ctx, input, output);
            }
        };
        a.recv.open(output);
        return a;
    }

    @Override
    public void reduceReceive(int srcParition, int offset, int totalSize,
            byte[] bs, Object userObject) throws IMRUDataException {
        //        Rt.p(reducerInfo.aggrTree.operator + " recv from " + srcParition + " "
        //                + offset + "/" + totalSize);
        A a = (A) userObject;
        a.recv.receive(srcParition, offset, totalSize, bs);
    }

    @Override
    public void reduceRecvDbgInfo(int srcParition, ImruIterInfo info,
            Object userObject) throws IMRUDataException {
        //        Rt.p(reducerInfo.aggrTree.operator + " recv info ");
        A a = (A) userObject;
        a.info.add(info);
    }

    @Override
    public ImruIterInfo reduceClose(Object userObject) throws IMRUDataException {
        //        Rt.p(reducerInfo.aggrTree.operator + " close ");
        A a = (A) userObject;
        a.recv.close();
        return a.info;
    }

    /**
     * Combine multiple raw data to one binary data
     */
    abstract public void reduceFrames(IMRUReduceContext ctx,
            Iterator<byte[]> input, OutputStream output)
            throws IMRUDataException;

    @Override
    public Object updateInit(final IMRUContext ctx, final Model model)
            throws IMRUDataException {
        A a = new A();
        a.info = new ImruIterInfo(ctx);
        a.recv = new SerializedFrames.Receiver() {
            @Override
            public void process(Iterator<byte[]> input)
                    throws IMRUDataException {
                updatedModel = updateFrames(ctx, input, model);
            }
        };
        a.recv.open(null);
        return a;
    }

    @Override
    public void updateReceive(int srcParition, int offset, int totalSize,
            byte[] bs, Object userObject) throws IMRUDataException {
        A a = (A) userObject;
        a.recv.receive(srcParition, offset, totalSize, bs);
    }

    @Override
    public void updateRecvInformation(int srcParition, ImruIterInfo info,
            Object userObject) throws IMRUDataException {
        A a = (A) userObject;
        a.info.add(info);
    }

    @Override
    public ImruIterInfo updateClose(Object userObject) throws IMRUDataException {
        A a = (A) userObject;
        a.recv.close();
        return a.info;
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
