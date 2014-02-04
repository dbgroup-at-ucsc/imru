package edu.uci.ics.hyracks.imru.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.IJobSerializerDeserializer;
import edu.uci.ics.hyracks.control.nc.application.NCApplicationContext;
import edu.uci.ics.hyracks.imru.data.SerializedFrames;
import edu.uci.ics.hyracks.imru.dataflow.IMRUSerialize;

abstract public class ImruStream<Model extends Serializable, Data extends Serializable>
        implements Serializable {
    DeploymentId deploymentId;

    public void setDeploymentId(DeploymentId deploymentId) {
        this.deploymentId = deploymentId;
    }

    /**
     * Frame size must be large enough to store at least one tuple
     */
    public int getCachedDataFrameSize() {
        return 1024 * 1024;
    }

    /**
     * Parse input data and output binary data, called if data is cached in disk
     */
    abstract public void parse(IMRUContext ctx, InputStream in,
            FrameWriter writer) throws IOException;

    /**
     * Parse into memory cache
     */
    abstract public void parse(IMRUContext ctx, InputStream input,
            DataWriter<Data> output) throws IOException;

    /**
     * For a list of binary data, return one binary data
     */
    abstract public ImruIterInfo map(IMRUContext ctx,
            Iterator<ByteBuffer> input, Model model, OutputStream output,
            int cachedDataFrameSize) throws IMRUDataException;

    /**
     * For a list of in memory data, return one binary data
     */
    abstract public ImruIterInfo mapMem(IMRUContext ctx, Iterator<Data> input,
            Model model, OutputStream output, int cachedDataFrameSize)
            throws IMRUDataException;

    abstract public Object reduceInit(IMRUReduceContext ctx, OutputStream output)
            throws IMRUDataException;

    abstract public void reduceReceive(int srcParition, int offset,
            int totalSize, byte[] bs, Object userObject)
            throws IMRUDataException;

    abstract public void reduceRecvDbgInfo(int srcParition,
            ImruIterInfo information, Object userObject)
            throws IMRUDataException;

    abstract public ImruIterInfo reduceClose(Object userObject)
            throws IMRUDataException;

    abstract public Object updateInit(IMRUContext ctx, Model model)
            throws IMRUDataException;

    abstract public void updateReceive(int srcParition, int offset,
            int totalSize, byte[] bs, Object userObject)
            throws IMRUDataException;

    abstract public void updateRecvInformation(int srcParition,
            ImruIterInfo info, Object userObject) throws IMRUDataException;

    abstract public ImruIterInfo updateClose(Object userObject)
            throws IMRUDataException;

    abstract public Model getUpdatedModel() throws IMRUDataException;

    /**
     * Return true to exit loop
     */
    abstract public boolean shouldTerminate(Model model, ImruIterInfo info);

    /**
     * Callback function when some nodes failed. User should decide what action to take
     * 
     * @param completedRanges
     *            successfully processed ranges of the data
     * @param dataSize
     *            the total size of the data
     * @param optimalNodesForRerun
     *            optimal number of nodes to rerun the iteration
     * @param rerunTime
     *            the estimated time to rerun the iteration
     * @param optimalNodesForPartiallyRerun
     *            optimal number of nodes to rerun only the unprocessed data
     * @param partiallyRerunTime
     *            the estimated time to rerun only the unprocessed data
     * @return action to take
     */
    public RecoveryAction onJobFailed(List<ImruSplitInfo> completedRanges,
            long dataSize, int optimalNodesForRerun, float rerunTime,
            int optimalNodesForPartiallyRerun, float partiallyRerunTime) {
        return RecoveryAction.Rerun;
    }

    /**
     * Integrates two partially completed model to one model
     * 
     * @param model1
     * @param model2
     * @return
     */
    public Model integrate(Model model1, Model model2) {
        return model1;
    }

    public Object reduceDbgInfoInit(final IMRUContext ctx,
            final Object userObject) throws IMRUDataException {
        SerializedFrames.Receiver reducerDbgInfo = new SerializedFrames.Receiver(
                "reduceDbg") {
            @Override
            public void receiveComplete(int srcPartition, byte[] bs)
                    throws IMRUDataException {
                ImruIterInfo info = (ImruIterInfo) ctx.deserialize(
                        deploymentId, bs);
                reduceRecvDbgInfo(srcPartition, info, userObject);
            }

            @Override
            public void process(Iterator<byte[]> input, OutputStream output)
                    throws IMRUDataException {
            }
        };
        reducerDbgInfo.open();
        return reducerDbgInfo;
    }

    public boolean reduceDbgInfoReceive(int srcParition, int offset,
            int totalSize, byte[] bs, Object userObject)
            throws IMRUDataException {
        SerializedFrames.Receiver reducerDbgInfo = (SerializedFrames.Receiver) userObject;
        return reducerDbgInfo.receive(srcParition, offset, totalSize, bs);
    }

    public void reduceDbgInfoClose(Object userObject) throws IMRUDataException {
        SerializedFrames.Receiver reducerDbgInfo = (SerializedFrames.Receiver) userObject;
        reducerDbgInfo.close();
    }

    public Object updateDbgInfoInit(final IMRUContext ctx,
            final Object userObject) throws IMRUDataException {
        SerializedFrames.Receiver updateDbgInfo = new SerializedFrames.Receiver(
                "updateDbg") {
            @Override
            public void receiveComplete(int srcPartition, byte[] bs)
                    throws IMRUDataException {
                ImruIterInfo info = (ImruIterInfo) ctx.deserialize(
                        deploymentId, bs);
                updateRecvInformation(srcPartition, info, userObject);
            }

            @Override
            public void process(Iterator<byte[]> input, OutputStream output)
                    throws IMRUDataException {
            }
        };
        updateDbgInfo.open();
        return updateDbgInfo;
    }

    public void updateDbgInfoReceive(int srcParition, int offset,
            int totalSize, byte[] bs, Object userObject)
            throws IMRUDataException {
        SerializedFrames.Receiver updateDbgInfo = (SerializedFrames.Receiver) userObject;
        updateDbgInfo.receive(srcParition, offset, totalSize, bs);
    }

    public void updateDbgInfoClose(Object userObject) throws IMRUDataException {
        SerializedFrames.Receiver updateDbgInfo = (SerializedFrames.Receiver) userObject;
        updateDbgInfo.close();
    }
}
