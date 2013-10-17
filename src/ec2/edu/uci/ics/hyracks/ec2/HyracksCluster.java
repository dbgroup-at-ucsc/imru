package edu.uci.ics.hyracks.ec2;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.test0.GnuPlot;

public class HyracksCluster {
    public HyracksNode controller;
    public HyracksNode[] nodes;
    public HyracksNode[] allNodes;
    Hashtable<String, HyracksNode> nodeNameHash = new Hashtable<String, HyracksNode>();
    Hashtable<Integer, HyracksNode> nodeIdHash = new Hashtable<Integer, HyracksNode>();

    HyracksCluster() {
    }

    public HyracksCluster(String controller, String[] nodes, String user,
            File privateKey) {
        this.controller = new RegularNode(this, -1, controller, controller,
                "CC", user, privateKey);
        this.nodes = new HyracksNode[nodes.length];
        this.allNodes = new HyracksNode[nodes.length + 1];
        this.allNodes[0] = this.controller;
        for (int i = 0; i < nodes.length; i++) {
            this.allNodes[i + 1] = this.nodes[i] = new RegularNode(this, i,
                    nodes[i], nodes[i], "NC" + i, user, privateKey);
        }
        for (HyracksNode node : this.nodes) {
            nodeIdHash.put(node.nodeId, node);
            nodeNameHash.put(node.name, node);
        }
    }

    public void reportAllMemory(GnuPlot plot) throws Exception {
        int time = 0;
        SSH[] sshs = new SSH[nodes.length + 1];
        sshs[0] = controller.ssh();
        for (int i = 0; i < nodes.length; i++) {
            sshs[i + 1] = nodes[i].ssh();
        }
        try {
            while (true) {
                plot.startNewX(time++);
                for (int i = 0; i < sshs.length; i++) {
                    sshs[i].verbose = false;
                    String result = sshs[i].execute("free -m", true);
                    String[] lines = result.split("\n");
                    String[] ss = lines[2].split(" +");
                    plot.addY(Integer.parseInt(ss[3]));
                }
                plot.finish();
            }
        } finally {
            plot.finish();
            for (int i = 0; i < nodes.length; i++) {
                if (sshs[i] != null)
                    sshs[i].close();
            }
        }
    }

    /**
     * Wait until it's possible to ssh to all instances
     * 
     * @throws Exception
     */
    public void waitSSH() throws Exception {
        LinkedList<HyracksNode> queue = new LinkedList<HyracksNode>();
        for (HyracksNode node : nodes)
            queue.add(node);
        while (queue.size() > 0) {
            HyracksNode node = queue.remove();
            int n = 0;
            while (true) {
                try {
                    String result = node.ssh("whoami");
                    if (!result.contains("timed out")
                            && !result.contains("refused"))
                        break;
                } catch (Exception e) {
                    e.printStackTrace();
                }
                Thread.sleep(1000);
                if ((n % 5) == 0)
                    Rt.p(node.name + " hasn't started ssh yet");
                n++;
            }
        }
    }

    /**
     * Make sure all instances are added to known host list
     * 
     * @throws Exception
     */
    public void sshTest() throws Exception {
        for (HyracksNode node : nodes) {
            String result = node.ssh("whoami");
            if (result.contains("failed"))
                throw new Exception("host key verification of " + node.publicIp
                        + " failed");
        }
    }

    public void install(File hyracksEc2Root) throws Exception {
        for (HyracksNode node : nodes)
            node.install(hyracksEc2Root);
    }

    public String[] getNodeNames() {
        String[] ss = new String[nodes.length];
        for (int i = 0; i < ss.length; i++)
            ss[i] = nodes[i].name;
        return ss;
    }

    public String getAdminURL() {
        if (controller == null)
            return null;
        return "http://" + controller.publicIp + ":16001/adminconsole/";
    }

    static ExecutorService pool = java.util.concurrent.Executors
            .newCachedThreadPool();

    public void executeOnAllNode(final NodeCallback callback) throws Exception {
        Vector<Future> fs = new Vector<Future>();
        for (HyracksNode node : nodes) {
            final HyracksNode node2 = node;
            Future<Integer> f = pool.submit(new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    try {
                        callback.run(node2);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return 0;
                }
            });
            fs.add(f);
        }
        for (Future f : fs)
            f.get();
    }

    public void startHyrackCluster() throws Exception {
        controller.startCC();
        Thread.sleep(5000); // wait to avoid connection refuse exception
        for (int i = 0; i < 100; i++) {
            try {
                Socket hcc = new Socket(controller.publicIp, 3099);
                hcc.close();
                break;
            } catch (java.net.ConnectException e1) {
                if (!"Connection refused".equals(e1.getMessage()))
                    Rt
                            .p(e1.getClass().getSimpleName() + ": "
                                    + e1.getMessage());
            } catch (Exception e) {
                e.printStackTrace();
            }
            Thread.sleep(100);
        }
        executeOnAllNode(new NodeCallback() {
            @Override
            public void run(HyracksNode node) throws Exception {
                // Connection refused for some nodes
                Thread.sleep(node.nodeId * 1000);
                node.startNC();
            }
        });
    }

    public void stopHyrackCluster() throws Exception {
        controller.startCC();
        executeOnAllNode(new NodeCallback() {
            @Override
            public void run(HyracksNode node) throws Exception {
                node.stopNC();
            }
        });
    }

    public void uploadData(String[] localAndremote) throws Exception {
        String[] local = new String[localAndremote.length];
        String[] remote = new String[localAndremote.length];
        for (int i = 0; i < local.length; i++) {
            String[] ss = localAndremote[i].split("\t");
            local[i] = ss[0];
            remote[i] = ss[1];
        }
        uploadData(local, remote);
    }

    public void uploadData(String[] local, String[] remote) throws Exception {
        if (local.length != remote.length)
            throw new IOException("local.length!=remote.length");
        Vector<String> nodeNames = new Vector<String>();
        Hashtable<String, Vector<String>> hashtable = new Hashtable<String, Vector<String>>();
        for (int i = 0; i < local.length; i++) {
            String localPath = local[i];
            String remotePath = remote[i];
            int t = remotePath.indexOf(':');
            if (t < 0)
                throw new IOException(
                        "Please specify remote location in the <node>:<path> format. "
                                + remotePath);
            String nodeName = remotePath.substring(0, t);
            remotePath = remotePath.substring(t + 1);
            Vector<String> v = hashtable.get(nodeName);
            if (v == null) {
                v = new Vector<String>();
                nodeNames.add(nodeName);
                hashtable.put(nodeName, v);
            }
            v.add(localPath);
            v.add(remotePath);
        }
        for (String nodeName : nodeNames) {
            HyracksNode node = nodeNameHash.get(nodeName);
            Vector<String> v = hashtable.get(nodeName);
            String[] localPath = new String[v.size() / 2];
            String[] remotePath = new String[v.size() / 2];
            for (int i = 0; i < localPath.length; i++) {
                localPath[i] = v.get(i + i);
                remotePath[i] = v.get(i + i + 1);
            }
            node.uploadData(localPath, remotePath);
        }
    }

    public SSH ssh(int nodeId) throws Exception {
        HyracksNode node = nodeIdHash.get(nodeId);
        if (node == null)
            throw new Exception("Can't find node " + nodeId);
        return node.ssh();
    }

    public String getControllerPublicDnsName() {
        return controller.publicIp;
    }

    public String getNodePublicDnsName(int nodeId) throws IOException {
        HyracksNode node = nodeIdHash.get(nodeId);
        if (node == null)
            throw new IOException("Can't find node " + nodeId);
        return node.publicIp;
    }

    public void write(int nodeId, String path, byte[] data) throws Exception {
        SSH ssh = ssh(nodeId);
        try {
            ssh.put(path, new ByteArrayInputStream(data));
        } finally {
            ssh.close();
        }
    }

    public byte[] read(int nodeId, String path) throws Exception {
        SSH ssh = ssh(nodeId);
        try {
            return Rt.read(ssh.get(path));
        } finally {
            ssh.close();
        }
    }

    public HyracksConnection getHyracksConnection() throws Exception {
        return new HyracksConnection(controller.publicIp, 3099);
    }

    public void printProcesses(int id) throws Exception {
        if (id < 0) {
            for (HyracksNode node : nodes)
                node.printProcesses();
        } else {
            HyracksNode node = nodeIdHash.get(id);
            if (node != null)
                node.printProcesses();
        }
    }

    public void printLogs(int id, final int lines) throws Exception {
        if (id < 0) {
            controller.printLogs(lines);
            final Object sync = new Object();
            executeOnAllNode(new NodeCallback() {
                @Override
                public void run(HyracksNode node) throws Exception {
                    SSH ssh = node.ssh();
                    try {
                        ssh.verbose = false;
                        String result = ssh.execute("tail -n " + lines
                                + " /tmp/t2/logs/" + node.name + ".log", true);
                        synchronized (sync) {
                            Rt.p(node.name + " log: %s", result);
                        }
                    } finally {
                        ssh.close();
                    }
                }
            });
        } else {
            HyracksNode node = nodeIdHash.get(id);
            if (node != null)
                node.printLogs(lines);
        }
    }

    public void printOutputs(int id) throws Exception {
        if (id < 0) {
            for (HyracksNode node : nodes)
                node.printOutputs();
        } else {
            HyracksNode node = nodeIdHash.get(id);
            if (node != null)
                node.printOutputs();
        }
    }

    public void listDir(String path) throws Exception {
        for (HyracksNode node : nodes)
            node.listDir(path);
    }

    public void rmrDir(String path) throws Exception {
        for (HyracksNode node : nodes)
            node.rmrDir(path);
    }
}
