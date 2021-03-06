/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.imru.runtime.bootstrap;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.state.IStateObject;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IWorkspaceFileFactory;
import edu.uci.ics.hyracks.api.resources.IDeallocatable;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.control.nc.resources.DefaultDeallocatableRegistry;
import edu.uci.ics.hyracks.imru.file.HDFSSplit;

/**
 * Provides context that is shared between all operators in the IMRU
 * dataflow.
 */
public class IMRURuntimeContext implements IWorkspaceFileFactory {
    private Map<StateKey, IStateObject> appStateMap = new ConcurrentHashMap<StateKey, IStateObject>();
    private IOManager ioManager;
    private DefaultDeallocatableRegistry registry = new DefaultDeallocatableRegistry();

    public IMRURuntimeContext(INCApplicationContext appCtx) {
        if (appCtx != null)
            ioManager = (IOManager) appCtx.getRootContext().getIOManager();
    }

    public void close() {
        registry.close();
        appStateMap.clear();
    }

    /**
     * Lock used to control access to the shared model, to ensure that
     * only one Map task loads it from HDFS.
     */
    public final Object envLock = new Object();
    /**
     * One instance of the model is shared between all Map tasks on
     * the same machine; only one Map task loads the model from HDFS,
     * and the others use the shared copy.
     */
    public Serializable model = null;

    /**
     * Output writer shared in each node in the train-merge interface.
     */
    public Vector<IFrameWriter> writers = new Vector<IFrameWriter>();

    /**
     * The round that the current global model was loaded in.
     */
    public int modelAge = 0;

    /**
     * If an iteration failed and UDF ask to rerun only the failed
     * partitions. This variable shows the current recoveryIteration.
     * If this is not a recoveryIteration, the value is -1.
     */
    public int currentRecoveryIteration = -1;

    /**
     * Regular run: 0
     * Rerun: > 0
     */
    public int rerunNum = 0;

    public Hashtable<String, Object> userObjects = new Hashtable<String, Object>();

    /**
     * Data partitions for dynamic mapping
     */
    public LinkedList<HDFSSplit> queue = new LinkedList<HDFSSplit>();

    public Map<StateKey, IStateObject> getAppStateStore() {
        return appStateMap;
    }

    public static IMRURuntimeContext get(IHyracksTaskContext ctx) {
        return (IMRURuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject();
    }

    @Override
    public FileReference createManagedWorkspaceFile(String prefix)
            throws HyracksDataException {
        final FileReference fRef = ioManager.createWorkspaceFile(prefix);
        registry.registerDeallocatable(new IDeallocatable() {
            @Override
            public void deallocate() {
                fRef.delete();
            }
        });
        return fRef;
    }

    @Override
    public FileReference createUnmanagedWorkspaceFile(String prefix)
            throws HyracksDataException {
        return ioManager.createWorkspaceFile(prefix);
    }
}