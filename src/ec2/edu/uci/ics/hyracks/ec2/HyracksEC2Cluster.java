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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.ec2.model.StartInstancesResult;
import com.amazonaws.services.ec2.model.StopInstancesRequest;
import com.amazonaws.services.ec2.model.StopInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;

import edu.uci.ics.hyracks.api.client.HyracksConnection;

/**
 * @author wangrui
 */
public class HyracksEC2Cluster extends HyracksCluster {
    public static final String FULLSTACK_IMRU_IMAGE_ID = "ami-5eb02637";
    public static int MAX_COUNT = 3;
    public static final String HYRACKS_SECURITY_GROUP = "hyracks-security-group";
    public static final String OPENED_PORTS = "22,1099,3099,16001";
    public static String NODE_NAME_PREFIX = "hyracks-auto-deploy-";
    EC2Wrapper ec2;
    HyracksEC2Node ec2Controller;
    HyracksEC2Node[] ec2Nodes;
    String keyName;
    String instancePrefix;
    String securityGroup = HYRACKS_SECURITY_GROUP;
    String openPorts = OPENED_PORTS;

    String machineType = "t1.micro";
    // ubuntu image: ami-3d4ff254
    // hyracks-ec2: ami-5eb02637
    String imageId = FULLSTACK_IMRU_IMAGE_ID;

    public HyracksEC2Cluster(File credentialsFile, File privateKeyFile)
            throws Exception {
        this(credentialsFile, privateKeyFile, NODE_NAME_PREFIX);
    }

    public HyracksEC2Cluster(File credentialsFile, File privateKeyFile,
            String instancePrefix) throws Exception {
        if (!credentialsFile.exists())
            throw new IOException(
                    credentialsFile.getAbsolutePath()
                            + " doesn't exist.\r\n"
                            + "Insert your AWS Credentials from http://aws.amazon.com/security-credentials to a file with content\r\naccessKey=xx\r\n"
                            + "secretKey=xx");
        if (!privateKeyFile.exists())
            throw new Error(
                    "Key pair needed. Please create "
                            + "a key pair in https://console.aws.amazon.com/ec2/ and download it to "
                            + privateKeyFile.getParent() + "/");
        this.ec2 = new EC2Wrapper(credentialsFile, privateKeyFile
                .getParentFile());
        this.keyName = privateKeyFile.getName();
        if (this.keyName.indexOf('.') > 0)
            this.keyName = this.keyName.substring(0, this.keyName
                    .lastIndexOf('.'));
        this.instancePrefix = instancePrefix;
        refresh();
    }

    public void reload() {
        ec2.reload();
    }

    public String getMachineType() {
        return machineType;
    }

    public HyracksEC2Node[] getNodes() {
        return ec2Nodes;
    }

    public void setMachineType(String machineType) {
        this.machineType = machineType;
    }

    public String getImageId() {
        return imageId;
    }

    public void setImageId(String imageId) {
        this.imageId = imageId;
    }

    public String getSecurityGroup() {
        return securityGroup;
    }

    public void setSecurityGroup(String securityGroup) {
        this.securityGroup = securityGroup;
    }

    public String getOpenPorts() {
        return openPorts;
    }

    public void setOpenPorts(String openPorts) {
        this.openPorts = openPorts;
    }

    public void refresh() {
        Vector<HyracksEC2Node> v = new Vector<HyracksEC2Node>();
        HyracksEC2Node controller = null;
        for (Instance instance : ec2.listInstances(instancePrefix)) {
            String name = ec2.getName(instance);
            if (!name.startsWith(instancePrefix))
                throw new Error("not a instance belong to this cluster");
            HyracksEC2Node node = new HyracksEC2Node(this, Integer
                    .parseInt(name.substring(instancePrefix.length())),
                    instance);
            if (node.nodeId == 0)
                controller = node;
            v.add(node);
        }
        HyracksEC2Node[] nodes = v.toArray(new HyracksEC2Node[v.size()]);
        Arrays.sort(nodes, new Comparator<HyracksEC2Node>() {
            @Override
            public int compare(HyracksEC2Node o1, HyracksEC2Node o2) {
                return o1.nodeId - o2.nodeId;
            }
        });
        this.controller=this.ec2Controller = controller;
        this.nodes =  this.ec2Nodes = nodes;
        for (HyracksEC2Node node : nodes) {
            nodeIdHash.put(node.nodeId, node);
            nodeNameHash.put(node.name, node);
        }
    }

    public void createSecurityGroup() {
        String[] ss = openPorts.split(",");
        int[] is = new int[ss.length];
        for (int i = 0; i < is.length; i++)
            is[i] = Integer.parseInt(ss[i].trim());
        ec2.createSecurityGroup(securityGroup, "Hyracks Security Group", is);
    }

    public void setTotalInstances(int count) throws Exception {
        if (count > MAX_COUNT)
            throw new Error("For safety reason, please modify "
                    + this.getClass().getName() + ".MAX_COUNT first");
        if (ec2Nodes.length < count) {
            addInstances(count - ec2Nodes.length);
        } else if (ec2Nodes.length > count) {
            for (int i = count; i < ec2Nodes.length; i++) {
                ec2Nodes[i].stopNC();
                ec2Nodes[i].terminateInstance();
            }
            refresh();
        }
    }

    /**
     * @param state
     *            pending, running, shutting-down, terminated, stopping, stopped
     * @return
     */
    public int getTotalMachines(String state) {
        refresh();
        int pending = 0;
        for (HyracksEC2Node node : ec2Nodes) {
            //pending, running, shutting-down, terminated, stopping, stopped
            String s = node.instance.getState().getName();
            if (state.equals(s))
                pending++;
        }
        return pending;
    }

    public void waitForInstance(String state) throws Exception {
        int n = 0;
        while (true) {
            int pending = getTotalMachines(state);
            if (pending == 0)
                return;
            if ((n % 5) == 0) {
                Rt.p(pending + " machines are " + state);
                ec2.reload();
            }
            n++;
            Thread.sleep(1000);
        }
    }

    public void waitForInstanceStart() throws Exception {
        // takes up to 30 seconds
        waitForInstance("pending");
    }

    public void waitForInstanceStop() throws Exception {
        // takes up to 30 seconds
        waitForInstance("stopping");
        //        waitForInstance("shutting-down");
    }

    public void addInstances(int count) {
        if (nodes.length + count > MAX_COUNT)
            throw new Error("For safety reason, please modify "
                    + this.getClass().getName() + ".MAX_COUNT first");

        createSecurityGroup();
        //        ec2.setEndpoint("ec2.us-east-1.amazonaws.com");

        RunInstancesRequest runInstancesRequest = new RunInstancesRequest()
                .withInstanceType(machineType).withImageId(imageId)
                .withMinCount(count).withMaxCount(count).withSecurityGroupIds(
                        securityGroup).withKeyName(keyName);

        RunInstancesResult runInstances = ec2.ec2
                .runInstances(runInstancesRequest);

        BitSet bs = new BitSet();
        for (HyracksEC2Node node : ec2Nodes)
            bs.set(node.nodeId);
        List<Instance> instances = runInstances.getReservation().getInstances();
        int idx = 0;
        for (Instance instance : instances) {
            idx = bs.nextClearBit(idx);
            CreateTagsRequest createTagsRequest = new CreateTagsRequest();
            createTagsRequest.withResources(instance.getInstanceId()) //
                    .withTags(new Tag("Name", instancePrefix + idx));
            ec2.ec2.createTags(createTagsRequest);
            bs.set(idx);
            idx++;
        }
        refresh();
    }

    public void startInstances() {
        refresh();
        Vector<String> instanceIds = new Vector<String>();
        for (HyracksEC2Node node : ec2Nodes) {
            //pending, running, shutting-down, terminated, stopping, stopped
            String state = node.instance.getState().getName();
            if ("stopped".equals(state))
                instanceIds.add(node.instance.getInstanceId());
        }
        StartInstancesRequest startInstancesRequest = new StartInstancesRequest()
                .withInstanceIds(instanceIds);
        StartInstancesResult result = ec2.ec2
                .startInstances(startInstancesRequest);
        Rt.p(result);
    }

    public void stopInstances() {
        Vector<String> instanceIds = new Vector<String>();
        for (HyracksEC2Node node : ec2Nodes)
            instanceIds.add(node.instance.getInstanceId());
        StopInstancesRequest stopInstancesRequest = new StopInstancesRequest()
                .withForce(false).withInstanceIds(instanceIds);
        StopInstancesResult result = ec2.ec2
                .stopInstances(stopInstancesRequest);
        Rt.p(result);
    }

    /**
     * terminate (delete) all instances
     */
    public void terminateInstances() {
        Vector<String> instanceIds = new Vector<String>();
        for (HyracksEC2Node node : ec2Nodes)
            instanceIds.add(node.instance.getInstanceId());
        TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest()
                .withInstanceIds(instanceIds);
        TerminateInstancesResult result = ec2.ec2
                .terminateInstances(terminateInstancesRequest);
        Rt.p(result);
    }

    public void printNodeStatus() {
        refresh();
        Rt.np("Hyracks EC2 Nodes (" + instancePrefix + "):");
        int pending = 0;
        int running = 0;
        int shutting = 0;
        int stopping = 0;
        int stopped = 0;
        for (HyracksEC2Node node : ec2Nodes) {
            //pending, running, shutting-down, terminated, stopping, stopped
            String state = node.instance.getState().getName();
            Rt.np(node.name + ": " + state + " "
                    + node.instance.getPrivateIpAddress() + " "
                    + node.instance.getPublicDnsName());
            if ("pending".equals(state))
                pending++;
            if ("running".equals(state))
                running++;
            if ("shutting".equals(state))
                shutting++;
            if ("stopping".equals(state))
                stopping++;
            if ("stopped".equals(state))
                stopped++;
        }
        Rt.np("pending: " + pending);
        Rt.np("running: " + running);
        Rt.np("shutting-down: " + shutting);
        Rt.np("stopping: " + stopping);
        Rt.np("stopped: " + stopped);
    }

    public String getClusterControllerPublicDnsName() {
        if (controller == null)
            return null;
        return ec2Controller.instance.getPublicDnsName();
    }
}
