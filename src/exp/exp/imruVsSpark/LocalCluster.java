package exp.imruVsSpark;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.URL;
import java.util.Stack;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.ec2.HyracksCluster;
import edu.uci.ics.hyracks.ec2.HyracksNode;
import edu.uci.ics.hyracks.ec2.NodeCallback;
import edu.uci.ics.hyracks.ec2.SSH;
import edu.uci.ics.hyracks.imru.util.Rt;

public class LocalCluster {
    public String user;
    public HyracksCluster cluster;
    String home;

    public LocalCluster(HyracksCluster cluster, String user) {
        this.cluster = cluster;
        this.user = user;
        home = "/home/" + user;
    }

    public int getSparkPort() throws Exception {
        int webport = 8080; //Spark didn't release the port for a long time
        //        File file = new File("/tmp/cache/sparkWebUI.txt");
        //        if (file.exists())
        //            webport = Integer.parseInt(Rt.readFile(file)) + 2;
        return webport;
    }

    public void startSpark() throws Exception {
        final int webport = getSparkPort();
        File file = new File("/tmp/cache/sparkWebUI.txt");
        Rt.write(file, ("" + webport).getBytes());
        //rsync -vrultzCc  /home/wangrui/ucscImru/bin/exp/test0/ ubuntu@ec2-54-242-134-180.compute-1.amazonaws.com:"+home+"/test/bin/exp/test0/
        Rt.p("Starting spark");
        Rt.p("http://" + cluster.controller.publicIp + ":8080/");
        final String master = cluster.controller.internalIp;
        final String sshCmd = "ssh -l ubuntu -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no ";
        {
            HyracksNode node = cluster.controller;
            Rt.p(node.getName());
            String result = Rt.runCommand(sshCmd + node.internalIp
                    + " \"ps aux | grep spark\"");
            boolean hasMaster = result.contains("spark.deploy.master.Master");
            if (!hasMaster) {
                String cmd = home
                        + "/spark-0.7.0/run spark.deploy.master.Master -i "
                        + master + " -p 7077 --webui-port " + webport
                        + " > masterSpark.log 2>&1 &";
                cmd = "SPARK_MASTER_IP=" + master + " " + home
                        + "/spark-0.8.0-incubating/bin/start-master.sh"
                        + " > masterSpark.log 2>&1 &";
                Rt.np(cmd);
                Rt.runCommand(sshCmd + node.internalIp + " \"" + cmd + "\"");
            }
        }
        Rt.sleep(3000);
        waitSparkServer();
        cluster.executeOnAllNode(new NodeCallback() {
            @Override
            public void run(HyracksNode node) throws Exception {
                Thread.sleep(100 * node.nodeId);
                Rt.p(node.getName());
                String result = Rt.runCommand(sshCmd + node.internalIp
                        + " \"ps aux | grep spark\"");
                boolean hasWorker = result
                        .contains("spark.deploy.worker.Worker");
                if (!hasWorker) {
                    String cmd = home
                            + "/spark-0.7.0/run spark.deploy.worker.Worker -i "
                            + node.internalIp + " spark://" + master
                            + ":7077 --webui-port " + (webport + 1)
                            + "  > slaveSpark.log 2>&1 &";
                    cmd = home + "/spark-0.8.0-incubating/spark-class"
                            + " org.apache.spark.deploy.worker.Worker"
                            + " spark://" + master
                            + ":7077 > slaveSpark.log 2>&1 &";
                    Rt.np(cmd);
                    Rt
                            .runCommand(sshCmd + node.internalIp + " \"" + cmd
                                    + "\"");
                }
            }
        });
        Thread.sleep(3000);
    }

    public void stopSpark() throws Exception {
        Rt.p("http://" + cluster.controller.publicIp + ":8080/");
        SSH ssh = cluster.controller.ssh();
        String cmd = "" + home + "/spark-0.7.0/bin/stop-all.sh";
        cmd = "ps aux | grep spark";
        Rt.np(cmd);
        for (String line : ssh.execute(cmd, true).split("\n")) {
            if (line.contains("spark.deploy.master.Master")) {
                String[] ss = line.split("[ |\t]+");
                int pid = Integer.parseInt(ss[1]);
                ssh.execute("kill " + pid);
            }
        }
        ssh.close();
    }

    public void startStratosphere() throws Exception {
        final int webport = getSparkPort();
        Rt.p("Starting stratosphere");
        final String master = cluster.controller.internalIp;
        final String sshCmd = "ssh -l ubuntu -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no ";
        final String config = "jobmanager.rpc.address: " + master + "\n"
                + "jobmanager.rpc.port: 6123\n"
                + "jobmanager.profiling.enable: false\n"
                + "jobmanager.rpc.numhandler: 8\n"
                + "jobmanager.heap.mb: 256\n" + "taskmanager.rpc.port: 6122\n"
                + "taskmanager.heap.mb: 512\n"
                + "channel.network.numberOfBuffers: 2048\n"
                + "channel.network.bufferSizeInBytes: 32768\n"
                + "pact.parallelization.degree: -1\n"
                + "pact.parallelization.max-intra-node-degree: -1\n"
                + "pact.parallelization.maxmachines: -1\n"
                + "pact.web.port: 8080\n"
                + "pact.web.rootpath: ./resources/web-docs/\n";
        {
            HyracksNode node = cluster.controller;
            Rt.p(node.getName());
            SSH ssh = node.ssh();
            Rt.p("uploading config");
            ssh.put("/home/ubuntu/stratosphere/conf/stratosphere-conf.yaml",
                    config.getBytes());
            Rt.p("finished");
            String result = Rt.runCommand(sshCmd + node.internalIp
                    + " \"ps aux | grep stratosphere | grep nephele\"");
            Rt.p(result);
            boolean hasMaster = result.contains("jobmanager");
            Rt.p(hasMaster);
            if (!hasMaster) {
                String cmd = home
                        + "/stratosphere/bin/nephele-jobmanager.sh start cluster > stratosphereJob.log 2>&1 &";
                Rt.np(cmd);
                Rt.runCommand(sshCmd + node.internalIp + " \"" + cmd + "\"");
            }
        }
        Rt.sleep(3000);
        cluster.executeOnAllNode(new NodeCallback() {
            @Override
            public void run(HyracksNode node) throws Exception {
                Thread.sleep(100 * node.nodeId);
                SSH ssh = node.ssh();
                ssh
                        .put(
                                "/home/ubuntu/stratosphere/conf/stratosphere-conf.yaml",
                                config.getBytes());
                ssh.close();
                Rt.p(node.getName());
                String result = Rt.runCommand(sshCmd + node.internalIp
                        + " \"ps aux | grep stratosphere | grep nephele\"");
                boolean hasWorker = result.contains("taskmanager");
                if (!hasWorker) {
                    String cmd = home
                            + "/stratosphere/bin/nephele-taskmanager.sh start > stratosphereTask.log 2>&1 &";
                    Rt.np(cmd);
                    Rt
                            .runCommand(sshCmd + node.internalIp + " \"" + cmd
                                    + "\"");
                }
            }
        });
        Thread.sleep(3000);
    }

    public void stopStratosphere() throws Exception {
        Rt.p("http://" + cluster.controller.publicIp + ":8080/");
        SSH ssh = cluster.controller.ssh();
        String cmd = home + "/stratosphere/bin/nephele-jobmanager.sh stop";
        cmd = "ps aux | grep stratosphere | grep nephele";
        Rt.np(cmd);
        for (String line : ssh.execute(cmd, true).split("\n")) {
            if (line.contains("nephele")) {
                String[] ss = line.split("[ |\t]+");
                int pid = Integer.parseInt(ss[1]);
                ssh.execute("kill " + pid);
            }
        }
        ssh.close();
    }

    public void setNetworkSpeed(final int kbit) throws Exception {
        cluster.executeOnAllNode(new NodeCallback() {
            @Override
            public void run(HyracksNode node) throws Exception {
                SSH ssh = node.ssh();
                ssh.execute("sudo wondershaper eth0 clear");
                if (kbit > 0)
                    ssh.execute("sudo wondershaper eth0 " + kbit + " " + kbit);
                ssh.close();
            }
        });
    }

    static ExecutorService pool = java.util.concurrent.Executors
            .newCachedThreadPool();

    public void stopAll() throws Exception {
        Rt.p("Stopping Spark, Hyracks and Stratosphere");
        cluster.executeOnAllNode(new NodeCallback() {
            @Override
            public void run(HyracksNode node) throws Exception {
                SSH ssh = node.ssh();
                ssh.verbose = false;
                for (String line : ssh.execute("ps aux | grep spark", true)
                        .split("\n")) {
                    if (line.contains("spark.deploy.master.Master")
                            || line.contains("spark.deploy.worker.Worker")) {
                        String[] ss = line.split("[ |\t]+");
                        int pid = Integer.parseInt(ss[1]);
                        ssh.execute("kill " + pid);
                    }
                }
                for (String line : ssh.execute("ps aux | grep Xrunjdwp", true)
                        .split("\n")) {
                    if (line.contains("hyracks")) {
                        String[] ss = line.split("[ |\t]+");
                        int pid = Integer.parseInt(ss[1]);
                        ssh.execute("kill " + pid);
                    }
                }
                for (String line : ssh.execute("ps aux | grep java", true)
                        .split("\n")) {
                    if ((line.contains("hyracks") || line.contains("nephele"))
                            && !line.contains("Ec2Experiments")) {
                        String[] ss = line.split("[ |\t]+");
                        int pid = Integer.parseInt(ss[1]);
                        ssh.execute("kill " + pid);
                    }
                }
                ssh.execute("rm " + home + "/masterSpark.log " + home
                        + "/slaveSpark.log");
                ssh.execute("rm /tmp/t1/logs/* /tmp/t2/logs/*");
                ssh.close();
            }
        });
        Rt.p("All nodes stopped");
    }

    public void checkHyracks() throws Exception {
        HyracksConnection hcc = new HyracksConnection(
                cluster.controller.publicIp, 3099);
        for (int i = 0; i < 20; i++) {
            //            Rt.p(hcc.getNodeControllerInfos());
            if (hcc.getNodeControllerInfos().size() == cluster.nodes.length)
                return;
            Rt.p("Only " + hcc.getNodeControllerInfos().size() + " NC");
            Thread.sleep(1000);
        }
        cluster.printLogs(-1, 100);
        throw new Error("Only " + hcc.getNodeControllerInfos().size() + " NC");
    }

    public void waitSparkServer() throws Exception {
        URL url = new URL("http://" + cluster.controller.publicIp + ":"
                + getSparkPort() + "/");
        InputStream in = null;
        for (int i = 0; i < 100; i++) {
            try {
                java.net.URLConnection connection = url.openConnection();
                in = connection.getInputStream();
                break;
            } catch (ConnectException e1) {
                Thread.sleep(500);
            } catch (java.io.IOException e1) {
                e1.printStackTrace();
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public int getSparkWorkers() throws Exception {
        URL url = new URL("http://" + cluster.controller.publicIp + ":"
                + getSparkPort() + "/");
        InputStream in = null;
        for (int i = 0; i < 100; i++) {
            try {
                java.net.URLConnection connection = url.openConnection();
                in = connection.getInputStream();
                break;
            } catch (java.io.IOException e1) {
                System.err.println(e1.getClass().getSimpleName() + ": "
                        + e1.getMessage());
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        if (in == null)
            throw new IOException("Can't open " + url);
        byte[] bs = Rt.read(in);
        String html = new String(bs);
        int t = html.indexOf("Workers:");
        html = html.substring(t);
        html = html.substring(0, html.indexOf("</li>"));
        html = html.substring(html.lastIndexOf('>') + 1).trim();
        return Integer.parseInt(html);
    }

    public void checkSpark() throws Exception {
        //18 seconds for 20 nodes
        for (int i = 0; i < 50; i++) {
            if (getSparkWorkers() == cluster.nodes.length)
                return;
            Rt.p("Only " + getSparkWorkers() + " Workers");
            Thread.sleep(1000);
        }
        throw new Error("Only " + getSparkWorkers() + " Workers");
    }
}
