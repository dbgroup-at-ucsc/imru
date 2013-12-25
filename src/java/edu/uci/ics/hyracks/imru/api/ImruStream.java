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

abstract public class ImruStream<Model extends Serializable, Data extends Serializable>
        implements Serializable {
    DeploymentId deploymentId;
    private SerializedFrames.Receiver reducerDbgInfo;
    private SerializedFrames.Receiver updateDbgInfo;

    public void setDeploymentId(DeploymentId deploymentId) {
        this.deploymentId = deploymentId;
    }

    public Serializable deserialize(IMRUContext ctx, byte[] bs)
            throws IMRUDataException {
        try {
            NCApplicationContext appContext = (NCApplicationContext) ctx
                    .getJobletContext().getApplicationContext();
            IJobSerializerDeserializer jobSerDe = appContext
                    .getJobSerializerDeserializerContainer()
                    .getJobSerializerDeserializer(deploymentId);
            return (Serializable) jobSerDe.deserialize(bs);
        } catch (HyracksException e) {
            throw new IMRUDataException(e);
        }
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

    abstract public void reduceInit(IMRUReduceContext ctx, OutputStream output)
            throws IMRUDataException;

    abstract public void reduceReceive(int srcParition, int offset,
            int totalSize, byte[] bs) throws IMRUDataException;

    abstract public void reduceRecvInformation(int srcParition,
            ImruIterInfo information) throws IMRUDataException;

    abstract public ImruIterInfo reduceClose() throws IMRUDataException;

    abstract public void updateInit(IMRUContext ctx, Model model)
            throws IMRUDataException;

    abstract public void updateReceive(int srcParition, int offset,
            int totalSize, byte[] bs) throws IMRUDataException;

    abstract public void updateRecvInformation(int srcParition,
            ImruIterInfo info) throws IMRUDataException;

    abstract public ImruIterInfo updateClose() throws IMRUDataException;

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

    public void reduceDbgInfoInit(final IMRUContext ctx)
            throws IMRUDataException {
        reducerDbgInfo = new SerializedFrames.Receiver() {
            @Override
            public void receiveComplete(int srcPartition, byte[] bs)
                    throws IMRUDataException {
                ImruIterInfo info = (ImruIterInfo) deserialize(ctx, bs);
                reduceRecvInformation(srcPartition, info);
            }

            @Override
            public void process(Iterator<byte[]> input, OutputStream output)
                    throws IMRUDataException {
            }
        };
        reducerDbgInfo.open();
    }

    public void reduceDbgInfoReceive(int srcParition, int offset,
            int totalSize, byte[] bs) throws IMRUDataException {
        reducerDbgInfo.receive(srcParition, offset, totalSize, bs);
    }

    public void reduceDbgInfoClose() throws IMRUDataException {
        reducerDbgInfo.close();
    }

    public void updateDbgInfoInit(final IMRUContext ctx)
            throws IMRUDataException {
        updateDbgInfo = new SerializedFrames.Receiver() {
            @Override
            public void receiveComplete(int srcPartition, byte[] bs)
                    throws IMRUDataException {
                ImruIterInfo info = (ImruIterInfo) deserialize(ctx, bs);
                updateRecvInformation(srcPartition, info);
            }

            @Override
            public void process(Iterator<byte[]> input, OutputStream output)
                    throws IMRUDataException {
            }
        };
        updateDbgInfo.open();
    }

    public void updateDbgInfoReceive(int srcParition, int offset,
            int totalSize, byte[] bs) throws IMRUDataException {
        updateDbgInfo.receive(srcParition, offset, totalSize, bs);
    }

    public void updateDbgInfoClose() throws IMRUDataException {
        updateDbgInfo.close();
    }
}
