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

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.uci.ics.hyracks.api.constraints.Constraint;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.constraints.expressions.ConstantExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionLocationExpression;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.connectors.HashtableLocalityMap;
import edu.uci.ics.hyracks.dataflow.std.connectors.ILocalityMap;
import edu.uci.ics.hyracks.dataflow.std.connectors.LocalityAwareMToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.imru.api.ImruParameters;
import edu.uci.ics.hyracks.imru.api.ImruStream;
import edu.uci.ics.hyracks.imru.dataflow.ReduceOperatorDescriptor;

/**
 * Adds local reducers (one per machine) to the output of a Map
 * operator.
 */
public class LocalReducerFactory {

    /**
     * Add local reducers to the output of a map operator.
     * 
     * @param spec
     *            The JobSpecification.
     * @param producerOp
     *            The producer of the data being reduced.
     * @param producerPort
     *            The producer port that connects to the agg. tree
     * @param producerLocations
     *            Maps from producer partitions to NC names.
     * @param consumerOp
     *            The consumer of the reduce output
     * @param consumerPort
     *            The consumer port that connects to the agg. tree.
     * @param consumerConn
     *            Connects the consumer to the agg. tree.
     * @param imruSpec
     *            The IMRU spec used by the job
     */
    @SuppressWarnings( { "rawtypes" })
    public static void addLocalReducers(DeploymentId deploymentId,JobSpecification spec,
            IOperatorDescriptor producerOp, int producerPort,
            String[] producerLocations, IOperatorDescriptor consumerOp,
            int consumerPort, IConnectorDescriptor consumerConn,
            ImruStream imruSpec, ImruParameters parameters) {
        ReduceOperatorDescriptor localReducer = new ReduceOperatorDescriptor(
                deploymentId, spec, imruSpec, "LRed", parameters);
        localReducer.level = -1;
        localReducer.isLocal = true;
        // Construct the locality map used to route tuples to local
        // reducers:
        Set<String> ncsWithMapOperators = new HashSet<String>(Arrays
                .asList(producerLocations));
        // We need as many local combiners as NCs with Map operators:
        PartitionConstraintHelper.addPartitionCountConstraint(spec,
                localReducer, ncsWithMapOperators.size());
        // Assign a local reducer partition to each of those NCs:
        Map<String, Integer> ncToLocalReducerPartitionMap = new HashMap<String, Integer>();
        int reducerPartition = 0;
        for (String nc : ncsWithMapOperators) {
            spec.addUserConstraint(new Constraint(
                    new PartitionLocationExpression(localReducer
                            .getOperatorId(), reducerPartition),
                    new ConstantExpression(new String[] { nc })));
            ncToLocalReducerPartitionMap.put(nc, reducerPartition);
            reducerPartition++;
        }
        // Based on those assignments, construct the locality map used
        // by the locality-aware partitioning connector.
        BitSet localityBitset = new BitSet(ncsWithMapOperators.size()
                * producerLocations.length);
        for (int sender = 0; sender < producerLocations.length; sender++) {
            String mapOperatorPartitionNC = producerLocations[sender];
            int consumer = ncToLocalReducerPartitionMap
                    .get(mapOperatorPartitionNC);
            localityBitset.set((sender * ncToLocalReducerPartitionMap.size())
                    + consumer);
        }
        ILocalityMap reducerLocalityMap = new HashtableLocalityMap(
                localityBitset);

        // Connect things together:
        IConnectorDescriptor mapLocalReducerConn = new LocalityAwareMToNPartitioningConnectorDescriptor(
                spec, OneToOneTuplePartitionComputerFactory.INSTANCE,
                reducerLocalityMap);
        spec.connect(mapLocalReducerConn, producerOp, producerPort,
                localReducer, 0);
        spec.connect(consumerConn, localReducer, 0, consumerOp, consumerPort);
    }

    /**
     * @param operatorLocations
     *            The locations of all Map operators.
     * @return The number of local combiners used (the number of
     *         leaves of the aggregation tree when using local
     *         combiners).
     */
    public static int getReducerCount(String operatorLocations[]) {
        Set<String> ncsWithMapOperators = new HashSet<String>(Arrays
                .asList(operatorLocations));
        return ncsWithMapOperators.size();
    }
}
