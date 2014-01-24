/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.imru.example.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.common.controllers.CCConfig;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.imru.api.IMRUJobControl;
import edu.uci.ics.hyracks.imru.api.old.IIMRUJob;
import edu.uci.ics.hyracks.imru.api.old.IIMRUJob2;
import edu.uci.ics.hyracks.imru.data.DataSpreadDriver;
import edu.uci.ics.hyracks.imru.file.HDFSSplit;
import edu.uci.ics.hyracks.imru.runtime.IMRUDriver;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUConnection;
import edu.uci.ics.hyracks.imru.trainmerge.TrainMergeDriver;
import edu.uci.ics.hyracks.imru.trainmerge.TrainMergeJob;
import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * This class wraps IMRU common functions.
 * Example usage: <blockquote>
 * 
 * <pre>
 * Client&lt;Model, MapReduceResult&gt; client = new Client&lt;Model, MapReduceResult&gt;(args);
 * //start a in-process cluster for debugging 
 * //client.startClusterAndNodes();  
 * client.connect();
 * client.uploadApp();
 * IMRUJob job = new IMRUJob();
 * JobStatus status = client.run(job);
 * Model finalModel = client.getModel();
 * </pre>
 * 
 * </blockquote>
 * 
 * @author wangrui
 * @param <Model>
 *            IMRU model which will be used in map() and updated in update()
 * @param <T>
 *            Object which is generated by map(), aggregated in reduce() and
 *            used in update()
 */
public class ClientTrainMerge<Model extends Serializable> extends Client {
    public ClientTrainMerge(String[] args) throws CmdLineException {
        super(args);
    }

    /**
     * run job
     * 
     * @throws Exception
     */
    public static <Model extends Serializable> Model run(
            TrainMergeJob<Model> job, Model initialModel, String[] args)
            throws Exception {
        // create a client object, which handles everything
        ClientTrainMerge<Model> client = new ClientTrainMerge<Model>(args);

        client.init();

        HDFSSplit[] splits = client.control.getSplits(
                client.options.inputPaths, client.options.numSplits,
                client.options.minSplitSize, client.options.maxSplitSize);
        TrainMergeDriver<Model> driver = new TrainMergeDriver<Model>(
                client.hcc, client.deploymentId, client.control.imruConnection,
                job, initialModel, splits, client.control.confFactory);
        driver.modelFileName = client.options.modelFilename;
        JobStatus status = driver.run();

        if (status == JobStatus.FAILURE) {
            System.err.println("Job failed; see CC and NC logs");
            System.exit(-1);
        }
        return driver.getModel();
    }
}
