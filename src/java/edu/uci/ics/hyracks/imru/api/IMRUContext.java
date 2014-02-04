package edu.uci.ics.hyracks.imru.api;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.LinkedList;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.context.IHyracksJobletContext;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IJobSerializerDeserializer;
import edu.uci.ics.hyracks.control.nc.Joblet;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.control.nc.application.NCApplicationContext;
import edu.uci.ics.hyracks.imru.dataflow.IMRUSerialize;
import edu.uci.ics.hyracks.imru.file.HDFSSplit;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRURuntimeContext;
import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * @author Rui Wang
 */
public class IMRUContext {
    private String operatorName;
    private NodeControllerService nodeController;

    private String nodeId;
    protected IHyracksTaskContext ctx;
    int partition;
    int nPartition;

    //for multi-core
    IMRURuntimeContext runtimeContext;
    int frameSize;

    public IMRUContext(IHyracksTaskContext ctx, int partition, int nPartition) {
        this(ctx, null, partition, nPartition);
    }

    public IMRUContext(IHyracksTaskContext ctx, String operatorName,
            int partition, int nPartition) {
        this.ctx = ctx;
        this.operatorName = operatorName;
        this.partition = partition;
        this.nPartition = nPartition;

        IHyracksJobletContext jobletContext = ctx.getJobletContext();
        if (jobletContext instanceof Joblet) {
            this.nodeController = ((Joblet) jobletContext).getNodeController();
            this.nodeId = nodeController.getId();
        } else {
            Rt.p(jobletContext.getClass().getName());
        }
    }

    public IMRUContext(String nodeId, int frameSize,
            IMRURuntimeContext runtimeContext, String operatorName,
            int partition, int nPartition) {
        this.nodeId = nodeId;
        this.runtimeContext = runtimeContext;
        this.frameSize = frameSize;
        this.operatorName = operatorName;
        this.partition = partition;
        this.nPartition = nPartition;
    }

    public String getNodeId() {
        return nodeId;
    }

    public int getPartition() {
        return partition;
    }

    public int getPartitions() {
        return nPartition;
    }

    public NodeControllerService getNodeController() {
        return nodeController;
    }

    public String getOperatorName() {
        return operatorName;
    }

    public ByteBuffer allocateFrame() throws HyracksDataException {
        if (ctx == null)
            return ByteBuffer.allocate(frameSize);
        return ctx.allocateFrame();
    }

    public int getFrameSize() {
        if (ctx == null)
            return frameSize;
        return ctx.getFrameSize();
    }

    public IHyracksJobletContext getJobletContext() {
        if (ctx == null)
            return null;
        return ctx.getJobletContext();
    }

    public Serializable deserialize(DeploymentId deploymentId, byte[] bs)
            throws IMRUDataException {
        try {
            if (ctx == null)
                return (Serializable) IMRUSerialize.deserialize(bs);
            NCApplicationContext appContext = (NCApplicationContext) ctx
                    .getJobletContext().getApplicationContext();
            IJobSerializerDeserializer jobSerDe = appContext
                    .getJobSerializerDeserializerContainer()
                    .getJobSerializerDeserializer(deploymentId);
            return (Serializable) jobSerDe.deserialize(bs);
        } catch (Exception e) {
            throw new IMRUDataException(e);
        }
    }

    public IMRURuntimeContext getRuntimeContext() {
        if (ctx == null)
            return runtimeContext;
        else {
            INCApplicationContext appContext = getJobletContext()
                    .getApplicationContext();
            return (IMRURuntimeContext) appContext.getApplicationObject();
        }
    }

    /**
     * Get the model shared in each node controller
     * 
     * @return
     */
    public Serializable getModel() {
        IMRURuntimeContext context = getRuntimeContext();
        return context.model;
    }

    public void setUserObject(String key, Object value) {
        IMRURuntimeContext context = getRuntimeContext();
        context.userObjects.put(key, value);
    }

    public Object getUserObject(String key) {
        IMRURuntimeContext context = getRuntimeContext();
        return context.userObjects.get(key);
    }

    public LinkedList<HDFSSplit> getQueue() {
        IMRURuntimeContext context = getRuntimeContext();
        return context.queue;
    }

    public void addSplitsToQueue(HDFSSplit[] splits) {
        LinkedList<HDFSSplit> queue = getQueue();
        synchronized (queue) {
            for (HDFSSplit split : splits) {
                if (split.uuid < 0)
                    throw new Error();
                queue.add(split);
            }
        }
    }

    public HDFSSplit popSplitFromQueue() {
        LinkedList<HDFSSplit> queue = getQueue();
        synchronized (queue) {
            if (queue.size() == 0)
                return null;
            HDFSSplit split = queue.remove();
            if (split.uuid < 0)
                throw new Error();
            return split;
        }
    }

    public HDFSSplit removeSplitFromQueue(int uuid) {
        LinkedList<HDFSSplit> queue = getQueue();
        synchronized (queue) {
            if (queue.size() == 0)
                return null;
            HDFSSplit s = null;
            for (HDFSSplit split : queue) {
                if (split.uuid == uuid) {
                    s = split;
                    break;
                }
            }
            if (s != null)
                queue.remove(s);
            return s;
        }
    }

    /**
     * Get current iteration number. The first iteration is 0.
     * 
     * @return
     */
    public int getIterationNumber() {
        IMRURuntimeContext context = getRuntimeContext();
        return context.modelAge - 1;
    }

    /**
     * Get current recovery iteration number.
     * 
     * @return
     */
    public int getRecoverIterationNumber() {
        IMRURuntimeContext context = getRuntimeContext();
        return context.currentRecoveryIteration;
    }

    /**
     * Get current rerun iteration number.
     * 
     * @return
     */
    public int getRerunCount() {
        IMRURuntimeContext context = getRuntimeContext();
        return context.rerunNum;
    }

    /**
     * Set the model shared in each node controller
     */
    public void setModel(Serializable model) {
        IMRURuntimeContext context = getRuntimeContext();
        context.model = model;
    }

    /**
     * Set the model shared in each node controller
     */
    public void setModel(Serializable model, int age) {
        IMRURuntimeContext context = getRuntimeContext();
        context.model = model;
        context.modelAge = age;
    }

    public IHyracksTaskContext getHyracksTaskContext() {
        return ctx;
    }
}
