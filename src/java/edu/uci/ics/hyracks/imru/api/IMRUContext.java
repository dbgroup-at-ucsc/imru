package edu.uci.ics.hyracks.imru.api;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.LinkedList;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.context.IHyracksJobletContext;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.control.nc.Joblet;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
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
        return ctx.allocateFrame();
    }

    public int getFrameSize() {
        return ctx.getFrameSize();
    }

    public IHyracksJobletContext getJobletContext() {
        return ctx.getJobletContext();
    }

    public IMRURuntimeContext getRuntimeContext() {
        INCApplicationContext appContext = getJobletContext()
                .getApplicationContext();
        return (IMRURuntimeContext) appContext.getApplicationObject();
    }

    /**
     * Get the model shared in each node controller
     * 
     * @return
     */
    public Serializable getModel() {
        INCApplicationContext appContext = getJobletContext()
                .getApplicationContext();
        IMRURuntimeContext context = (IMRURuntimeContext) appContext
                .getApplicationObject();
        return context.model;
    }

    public void setUserObject(String key, Object value) {
        INCApplicationContext appContext = getJobletContext()
                .getApplicationContext();
        IMRURuntimeContext context = (IMRURuntimeContext) appContext
                .getApplicationObject();
        context.userObjects.put(key, value);
    }

    public Object getUserObject(String key) {
        INCApplicationContext appContext = getJobletContext()
                .getApplicationContext();
        IMRURuntimeContext context = (IMRURuntimeContext) appContext
                .getApplicationObject();
        return context.userObjects.get(key);
    }

    public LinkedList<HDFSSplit> getQueue() {
        INCApplicationContext appContext = getJobletContext()
                .getApplicationContext();
        IMRURuntimeContext context = (IMRURuntimeContext) appContext
                .getApplicationObject();
        return context.queue;
    }

    /**
     * Get current iteration number. The first iteration is 0.
     * 
     * @return
     */
    public int getIterationNumber() {
        INCApplicationContext appContext = getJobletContext()
                .getApplicationContext();
        IMRURuntimeContext context = (IMRURuntimeContext) appContext
                .getApplicationObject();
        return context.modelAge - 1;
    }

    /**
     * Get current recovery iteration number.
     * 
     * @return
     */
    public int getRecoverIterationNumber() {
        INCApplicationContext appContext = getJobletContext()
                .getApplicationContext();
        IMRURuntimeContext context = (IMRURuntimeContext) appContext
                .getApplicationObject();
        return context.currentRecoveryIteration;
    }

    /**
     * Get current rerun iteration number.
     * 
     * @return
     */
    public int getRerunCount() {
        INCApplicationContext appContext = getJobletContext()
                .getApplicationContext();
        IMRURuntimeContext context = (IMRURuntimeContext) appContext
                .getApplicationObject();
        return context.rerunNum;
    }

    /**
     * Set the model shared in each node controller
     */
    public void setModel(Serializable model) {
        INCApplicationContext appContext = getJobletContext()
                .getApplicationContext();
        IMRURuntimeContext context = (IMRURuntimeContext) appContext
                .getApplicationObject();
        context.model = model;
    }

    /**
     * Set the model shared in each node controller
     */
    public void setModel(Serializable model, int age) {
        INCApplicationContext appContext = getJobletContext()
                .getApplicationContext();
        IMRURuntimeContext context = (IMRURuntimeContext) appContext
                .getApplicationObject();
        context.model = model;
        context.modelAge = age;
    }

    public IHyracksTaskContext getHyracksTaskContext() {
        return ctx;
    }
}
