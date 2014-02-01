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

package edu.uci.ics.hyracks.imru.jobgen;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Random;
import java.util.UUID;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.connectors.LocalityAwareMToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNReplicatingConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.hdfs.api.IKeyValueParser;
import edu.uci.ics.hyracks.hdfs.api.IKeyValueParserFactory;
import edu.uci.ics.hyracks.hdfs.dataflow.HDFSReadOperatorDescriptor;
import edu.uci.ics.hyracks.imru.api.IIMRUDataGenerator;
import edu.uci.ics.hyracks.imru.api.ImruOptions;
import edu.uci.ics.hyracks.imru.api.ImruParameters;
import edu.uci.ics.hyracks.imru.api.ImruStream;
import edu.uci.ics.hyracks.imru.api.TupleWriter;
import edu.uci.ics.hyracks.imru.dataflow.DataGeneratorOperatorDescriptor;
import edu.uci.ics.hyracks.imru.dataflow.DataLoadOperatorDescriptor;
import edu.uci.ics.hyracks.imru.dataflow.HDFSBlockFormat;
import edu.uci.ics.hyracks.imru.dataflow.HDFSBlockWriter;
import edu.uci.ics.hyracks.imru.dataflow.HDFSOD;
import edu.uci.ics.hyracks.imru.dataflow.IMRUOperatorDescriptor;
import edu.uci.ics.hyracks.imru.dataflow.MapOperatorDescriptor;
import edu.uci.ics.hyracks.imru.dataflow.ReduceOperatorDescriptor;
import edu.uci.ics.hyracks.imru.dataflow.SpreadConnectorDescriptor;
import edu.uci.ics.hyracks.imru.dataflow.SpreadOD;
import edu.uci.ics.hyracks.imru.dataflow.UpdateOperatorDescriptor;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.DynamicMapOD;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.ImruRecvOD;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.ImruSendOD;
import edu.uci.ics.hyracks.imru.dataflow.dynamic.test.DynamicAggregationStressTest;
import edu.uci.ics.hyracks.imru.file.ConfigurationFactory;
import edu.uci.ics.hyracks.imru.file.IMRUInputSplitProvider;
import edu.uci.ics.hyracks.imru.file.HDFSSplit;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUConnection;
import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * Generates JobSpecifications for iterations of iterative map reduce
 * update, using no aggregation, a generic aggregation tree or an n-ary
 * aggregation tree. The reducers are
 * assigned to random NC's by the Hyracks scheduler.
 * 
 * @author Josh Rosen
 * @author Rui Wang
 */
public class IMRUJobFactory {
    public static enum AGGREGATION {
        NONE,
        GENERIC,
        NARY,
        AUTO,
    };

    public final ConfigurationFactory confFactory;
    //    String inputPaths;
    HDFSSplit[] inputSplits;
    HDFSSplit[][] allocatedSplits;
    String[] mapOperatorLocations;
    String[] mapNodesLocations;
    String[] mapAndUpdateNodesLocations;
    String modelNode; //on which node the model will be
    private final int fanIn;
    private final int reducerCount;
    private AGGREGATION aggType;
    UUID id = UUID.randomUUID();
    IMRUConnection imruConnection;
    public ImruParameters parameters;

    //    boolean dynamicAggr;
    //    public boolean disableSwapping = false;
    //    public int maxWaitTimeBeforeSwap = 1000;
    //    public boolean dynamicDebug;

    public IMRUJobFactory(IMRUConnection imruConnection,
            HDFSSplit[] inputSplits, ConfigurationFactory confFactory,
            String type, int fanIn, int reducerCount, ImruParameters parameters)
            throws IOException, InterruptedException {
        this(imruConnection, inputSplits, confFactory, aggType(type), fanIn,
                reducerCount, parameters);
    }

    public static AGGREGATION aggType(String type) {
        if (type.equals("none")) {
            return AGGREGATION.NONE;
        } else if (type.equals("generic")) {
            return AGGREGATION.GENERIC;
        } else if (type.equals("nary")) {
            return AGGREGATION.NARY;
        } else {
            throw new IllegalArgumentException("Invalid aggregation tree type");
        }
    }

    public IMRUJobFactory(IMRUJobFactory f, HDFSSplit[] incompletedSplits,
            AGGREGATION aggType, boolean dynamicAggr) throws IOException,
            InterruptedException {
        this(f.imruConnection, incompletedSplits, f.confFactory, aggType,
                f.fanIn, f.reducerCount, f.parameters);
    }

    /**
     * Construct a new DefaultIMRUJobFactory.
     * 
     * @param inputPaths
     *            A comma-separated list of paths specifying the input
     *            files.
     * @param confFactory
     *            A Hadoop configuration used for HDFS settings.
     * @param fanIn
     *            The number of incoming connections to each
     *            reducer (excluding the level farthest from
     *            the root).
     * @throws InterruptedException
     * @throws HyracksException
     */
    public IMRUJobFactory(IMRUConnection imruConnection,
            HDFSSplit[] inputSplits, ConfigurationFactory confFactory,
            AGGREGATION aggType, int fanIn, int reducerCount,
            ImruParameters parameters) throws IOException, InterruptedException {
        this.imruConnection = imruConnection;
        this.confFactory = confFactory;
        this.inputSplits = inputSplits;
        this.parameters = parameters;
        // For repeatability of the partition assignments, seed the
        // source of
        // randomness using the job id.
        Random random = new Random(id.getLeastSignificantBits());
        mapOperatorLocations = ClusterConfig.setLocationConstraint(null, null,
                inputSplits, random);
        HashSet<String> hashSet = new HashSet<String>(Arrays
                .asList(mapOperatorLocations));
        mapNodesLocations = hashSet.toArray(new String[0]);
        if (parameters.dynamicMapping) {
            //create the exact number of mapper as required
            int mapPerNode = parameters.dynamicMappersPerNode;
            String[] locations = mapOperatorLocations;
            mapOperatorLocations = new String[mapNodesLocations.length
                    * mapPerNode];
            Hashtable<String, Integer> nodeToPartition = new Hashtable<String, Integer>();
            for (int i = 0; i < mapNodesLocations.length; i++) {
                nodeToPartition.put(mapNodesLocations[i], i * mapPerNode);
                for (int j = 0; j < mapPerNode; j++) {
                    mapOperatorLocations[i * mapPerNode + j] = mapNodesLocations[i];
                }
            }
            LinkedList<HDFSSplit>[] vs = new LinkedList[mapOperatorLocations.length];
            for (int i = 0; i < vs.length; i++)
                vs[i] = new LinkedList<HDFSSplit>();
            //assign partitions to the first mapper on each node
            for (int i = 0; i < inputSplits.length; i++) {
                Integer partition = nodeToPartition.get(locations[i]);
                vs[partition].add(inputSplits[i]);
            }
            //equalize partitions on each node
            for (int i = 0; i < mapNodesLocations.length; i++) {
                int start = i * mapPerNode;
                LinkedList<HDFSSplit> v = vs[start];
                vs[start] = new LinkedList<HDFSSplit>();
                while (v.size() > 0) {
                    for (int j = 0; j < mapPerNode; j++) {
                        if (v.size() == 0)
                            break;
                        vs[start + j].add(v.remove());
                    }
                }
            }
            allocatedSplits = new HDFSSplit[mapOperatorLocations.length][];
            for (int i = 0; i < vs.length; i++) {
                allocatedSplits[i] = vs[i].toArray(new HDFSSplit[vs[i].size()]);
                Rt.p(mapOperatorLocations[i] + ":");
                for (HDFSSplit split : allocatedSplits[i])
                    Rt.np("\t" + split);
            }
        }
        mapAndUpdateNodesLocations = hashSet.toArray(new String[0]);
        modelNode = mapNodesLocations[0];
        if (aggType == AGGREGATION.AUTO) {
            if (inputSplits.length < 3)
                aggType = AGGREGATION.NONE;
            else
                aggType = AGGREGATION.NARY;
        }
        this.aggType = aggType;
        this.fanIn = fanIn;
        this.reducerCount = reducerCount;
        if (aggType == AGGREGATION.NONE) {
        } else if (aggType == AGGREGATION.GENERIC) {
            if (reducerCount < 1)
                throw new IllegalArgumentException(
                        "Must specify a nonnegative aggregator count");
        } else if (aggType == AGGREGATION.NARY) {
            if (fanIn <= 1)
                throw new IllegalArgumentException(
                        "Must specify fan in greater than 1");
        }
    }

    public UUID getId() {
        return id;
    }

    public JobConf getConf() throws IOException {
        JobConf conf = new JobConf();
        conf
                .addResource(new Path(confFactory.hadoopConfPath
                        + "/core-site.xml"));
        conf.addResource(new Path(confFactory.hadoopConfPath
                + "/mapred-site.xml"));
        conf
                .addResource(new Path(confFactory.hadoopConfPath
                        + "/hdfs-site.xml"));
        return conf;
    }

    public HDFSSplit[] getSplits() {
        return inputSplits;
    }

    public InputSplit[] getInputSplits() throws IOException {
        //            JobConf conf = getConf();
        //            FileInputFormat.setInputPaths(conf, inputPaths);
        //            conf.setInputFormat(HDFSBlockFormat.class);
        //            return conf.getInputFormat().getSplits(conf, 1);
        InputSplit[] splits = new InputSplit[inputSplits.length];
        for (int i = 0; i < splits.length; i++)
            splits[i] = inputSplits[i].getMapredSplit();
        return splits;
    }

    @SuppressWarnings("rawtypes")
    public JobSpecification generateDataGenerateJob(IIMRUDataGenerator generator)
            throws IOException {
        JobSpecification spec = new JobSpecification();
        IMRUOperatorDescriptor dataLoad = new DataGeneratorOperatorDescriptor(
                spec, generator, inputSplits, confFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, dataLoad,
                mapOperatorLocations);
        spec.addRoot(dataLoad);
        return spec;
    }

    /**
     * Construct a JobSpecificiation for caching IMRU input records.
     * 
     * @param model
     *            The IIMRUJobSpecification
     * @param id
     *            A UUID identifying the IMRU job that this iteration belongs to.
     * @return A JobSpecification for caching the IMRU training examples.
     * @throws IOException
     */
    @SuppressWarnings("rawtypes")
    public JobSpecification generateDataLoadJob(ImruStream model)
            throws IOException {
        JobSpecification spec = new JobSpecification();
        if (confFactory.useHDFS()) {
            InputSplit[] splits = getInputSplits();
            RecordDescriptor recordDesc = new RecordDescriptor(
                    new ISerializerDeserializer[] { UTF8StringSerializerDeserializer.INSTANCE });
            HDFSReadOperatorDescriptor readOperator = new HDFSReadOperatorDescriptor(
                    spec, recordDesc, getConf(), splits, mapOperatorLocations,
                    new HDFSBlockWriter());
            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
                    readOperator, mapOperatorLocations);

            IOperatorDescriptor writer = new DataLoadOperatorDescriptor(spec,
                    model, inputSplits, confFactory, true, parameters);
            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
                    readOperator, mapOperatorLocations);

            spec.connect(new OneToOneConnectorDescriptor(spec), readOperator,
                    0, writer, 0);
        } else {
            IMRUOperatorDescriptor dataLoad = new DataLoadOperatorDescriptor(
                    spec, model, inputSplits, confFactory, false, parameters);
            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
                    dataLoad, mapOperatorLocations);
            spec.addRoot(dataLoad);
        }
        return spec;
    }

    public JobSpecification generateModelSpreadJob(DeploymentId deploymentId,
            String modelPath, int roundNum) {
        return generateModelSpreadJob(deploymentId, mapAndUpdateNodesLocations,
                modelNode, imruConnection, modelPath, roundNum, null);
    }

    /**
     * @param mapNodesLocations
     *            The nodes which contains map operators
     * @param initialNode
     *            The node which downloads model and start distributing
     * @param imruConnection
     * @param modelName
     * @param modelAge
     * @param dataFilePath
     *            Save distributed data to a file instead of loading to memory
     * @return
     */
    public static JobSpecification generateModelSpreadJob(
            DeploymentId deploymentId, String[] mapNodesLocations,
            String initialNode, IMRUConnection imruConnection,
            String modelName, int modelAge, String dataFilePath) {
        JobSpecification job = new JobSpecification();
        //        job.setFrameSize(frameSize);
        SpreadGraph graph = new SpreadGraph(mapNodesLocations, initialNode);
        //        graph.print();
        SpreadOD last = null;
        for (int i = 0; i < graph.levels.length; i++) {
            SpreadGraph.Level level = graph.levels[i];
            String[] locations = level.getLocationContraint();
            SpreadOD op = new SpreadOD(deploymentId, job, graph.levels, i,
                    modelName, imruConnection, modelAge, dataFilePath);
            if (i > 0)
                job.connect(new SpreadConnectorDescriptor(job,
                        graph.levels[i - 1], level), last, 0, op, 0);
            PartitionConstraintHelper.addAbsoluteLocationConstraint(job, op,
                    locations);
            last = op;
        }
        job.addRoot(last);
        return job;
    }

    /**
     * Construct a JobSpecification for a single iteration of IMRU.
     * 
     * @param model
     *            The IIMRUJobSpecification
     * @param id
     *            A UUID identifying the IMRU job that this iteration belongs to.
     * @param roundNum
     *            The round number.
     * @param modelInPath
     *            The HDFS path to read the current model from.
     * @param modelOutPath
     *            The HDFS path to write the updated model to.
     * @return A JobSpecification for an iteration of IMRU.
     * @throws HyracksException
     */
    @SuppressWarnings( { "rawtypes", "unchecked" })
    public JobSpecification generateJob(ImruStream model,
            DeploymentId deploymentId, String modelName)
            throws HyracksException {

        JobSpecification spec = new JobSpecification();
        // Create operators
        // File reading and writing

        // IMRU Computation
        // We will have one Map operator per input file.
        IMRUOperatorDescriptor mapOperator = null;
        if (parameters.dynamicMapping) {
            if (aggType == AGGREGATION.NARY && parameters.dynamicAggr) {
                //                mapOperator = new DynamicMapOD(spec, model, allocatedSplits,
                //                        roundNum, recoverRoundNum, rerunNum, "dmap", noDiskCache,
                //                        parameters);
                //                PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
                //                        mapOperator, mapOperatorLocations);
                int[] targets = DynamicAggregationStressTest
                        .getAggregationTree(mapNodesLocations.length,
                                this.fanIn);
                ImruSendOD send = new ImruSendOD(spec, targets, model, "send",
                        parameters, modelName, imruConnection);
                send.splits = inputSplits;
                send.allocatedSplits = allocatedSplits;
                ImruRecvOD recv = new ImruRecvOD(spec, deploymentId, targets);
                spec.connect(new SpreadConnectorDescriptor(spec, null, null),
                        send, 0, recv, 0);
                PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
                        send, mapNodesLocations);
                PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
                        recv, mapNodesLocations);
                //                LocalReducerFactory.addLocalReducers(spec, mapOperator, 0,
                //                        mapOperatorLocations, send, 0,
                //                        new OneToOneConnectorDescriptor(spec), model,
                //                        parameters);
                spec.addRoot(recv);
                return spec;
            } else {
                throw new Error(
                        "Dynamic aggregation must be enabled for dynamic mapping.");
            }
        } else {
            mapOperator = new MapOperatorDescriptor(spec, model, inputSplits,
                    "map", parameters);
        }
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
                mapOperator, mapOperatorLocations);

        if (aggType == AGGREGATION.NARY && parameters.dynamicAggr) {
            int[] targets = DynamicAggregationStressTest.getAggregationTree(
                    mapNodesLocations.length, this.fanIn);
            ImruSendOD send = new ImruSendOD(spec, targets, model, "send",
                    parameters, modelName, imruConnection);
            ImruRecvOD recv = new ImruRecvOD(spec, deploymentId, targets);
            spec.connect(new SpreadConnectorDescriptor(spec, null, null), send,
                    0, recv, 0);
            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, send,
                    mapNodesLocations);
            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, recv,
                    mapNodesLocations);
            LocalReducerFactory.addLocalReducers(spec, mapOperator, 0,
                    mapOperatorLocations, send, 0,
                    new OneToOneConnectorDescriptor(spec), model, parameters);
            spec.addRoot(recv);
            return spec;
        }

        // Environment updating
        IMRUOperatorDescriptor updateOperator = new UpdateOperatorDescriptor(
                spec, model, modelName, imruConnection, parameters);
        PartitionConstraintHelper.addPartitionCountConstraint(spec,
                updateOperator, 1);
        // Make sure update operator can get the model
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
                updateOperator, mapOperatorLocations[0]);

        if (aggType == AGGREGATION.NONE) {
            // Connect things together
            IConnectorDescriptor mapReduceConn = new MToNReplicatingConnectorDescriptor(
                    spec);
            spec.connect(mapReduceConn, mapOperator, 0, updateOperator, 0);
        } else if (aggType == AGGREGATION.GENERIC) {
            // One level of reducers (ala Hadoop)
            IOperatorDescriptor reduceOperator = new ReduceOperatorDescriptor(
                    spec, model, "generic reducer", this.parameters);
            PartitionConstraintHelper.addPartitionCountConstraint(spec,
                    reduceOperator, reducerCount);

            // Set up the local combiners (machine-local reducers)
            IConnectorDescriptor mapReducerConn = new LocalityAwareMToNPartitioningConnectorDescriptor(
                    spec, OneToOneTuplePartitionComputerFactory.INSTANCE,
                    new RangeLocalityMap(mapOperatorLocations.length));
            LocalReducerFactory.addLocalReducers(spec, mapOperator, 0,
                    mapOperatorLocations, reduceOperator, 0, mapReducerConn,
                    model, parameters);

            // Connect things together
            IConnectorDescriptor reduceUpdateConn = new MToNReplicatingConnectorDescriptor(
                    spec);
            spec
                    .connect(reduceUpdateConn, reduceOperator, 0,
                            updateOperator, 0);
        } else if (aggType == AGGREGATION.NARY) {
            // Reduce aggregation tree.
            IConnectorDescriptor reduceUpdateConn = new MToNReplicatingConnectorDescriptor(
                    spec);
            ReduceAggregationTreeFactory.buildAggregationTree(spec,
                    mapOperator, 0, inputSplits.length, updateOperator, 0,
                    reduceUpdateConn, fanIn, true, mapOperatorLocations, model,
                    parameters);
        }

        spec.addRoot(updateOperator);
        return spec;
    }
}
