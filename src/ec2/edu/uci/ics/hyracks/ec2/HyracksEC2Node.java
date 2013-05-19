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
package edu.uci.ics.hyracks.ec2;

import java.io.File;
import java.util.Vector;

import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.StopInstancesRequest;
import com.amazonaws.services.ec2.model.StopInstancesResult;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;

/**
 * @author wangrui
 */
public class HyracksEC2Node extends HyracksNode {
    public static final String NAME_PREFIX = "NC";
    public static final String HYRACKS_PATH = "/home/ubuntu/hyracks-ec2";
    Instance instance;
    HyracksEC2Cluster cluster;

    public HyracksEC2Node(HyracksEC2Cluster cluster, int nodeId,
            Instance instance) {
        super(cluster, nodeId, instance.getPublicDnsName(), instance
                .getPrivateIpAddress(), NAME_PREFIX + nodeId);
        this.cluster = cluster;
        this.instance = instance;
    }

    public Instance getInstance() {
        return instance;
    }

    public SSH ssh() throws Exception {
        return cluster.ec2.ssh(instance);
    }

    @Override
    public String ssh(String cmd) throws Exception {
        return cluster.ec2.ssh(instance, cmd);
    }

    @Override
    public void rsync(SSH ssh, File localDir, String remoteDir)
            throws Exception {
        cluster.ec2.rsync(instance, ssh, localDir, remoteDir);
    }

    public void stopInstance() {
        Vector<String> instanceIds = new Vector<String>();
        instanceIds.add(instance.getInstanceId());
        StopInstancesRequest stopInstancesRequest = new StopInstancesRequest()
                .withForce(false).withInstanceIds(instanceIds);
        StopInstancesResult result = cluster.ec2.ec2
                .stopInstances(stopInstancesRequest);
        Rt.p(result);
    }

    /**
     * terminate (delete) all instances
     */
    public void terminateInstance() {
        Vector<String> instanceIds = new Vector<String>();
        instanceIds.add(instance.getInstanceId());
        TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest()
                .withInstanceIds(instanceIds);
        TerminateInstancesResult result = cluster.ec2.ec2
                .terminateInstances(terminateInstancesRequest);
        Rt.p(result);
    }
}
