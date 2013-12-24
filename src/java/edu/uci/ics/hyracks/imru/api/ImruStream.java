package edu.uci.ics.hyracks.imru.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import edu.uci.ics.hyracks.api.deployment.DeploymentId;

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

    abstract public void parse(IMRUContext ctx, InputStream input,
            DataWriter<Data> output) throws IOException;

    /**
     * For a list of binary data, return one binary data
     */
    abstract public void map(IMRUContext ctx, Iterator<ByteBuffer> input,
            Model model, OutputStream output, int cachedDataFrameSize)
            throws IMRUDataException;

    /**
     * For a list of in memory data, return one binary data
     */
    abstract public void mapMem(IMRUContext ctx, Iterator<Data> input,
            Model model, OutputStream output, int cachedDataFrameSize)
            throws IMRUDataException;

    abstract public void reduceInit(IMRUReduceContext ctx, OutputStream output)
            throws IMRUDataException;

    abstract public void reduceReceive(int srcParition, int offset,
            int totalSize, byte[] bs) throws IMRUDataException;

    abstract public void reduceClose() throws IMRUDataException;

    abstract public void updateInit(IMRUContext ctx, Model model,
            ImruIterationInformation runtimeInformation)
            throws IMRUDataException;

    abstract public void updateReceive(int srcParition, int offset,
            int totalSize, byte[] bs) throws IMRUDataException;

    abstract public Model updateClose() throws IMRUDataException;

    /**
     * Return true to exit loop
     */
    abstract public boolean shouldTerminate(Model model,
            ImruIterationInformation runtimeInformation);

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
}
