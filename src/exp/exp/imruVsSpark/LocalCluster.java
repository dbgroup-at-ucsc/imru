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

    static ExecutorService pool = java.util.concurrent.Executors
            .newCachedThreadPool();

    public void stopAll() throws Exception {
        Rt.p("Stopping Spark and Hyracks");
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
                    if (line.contains("hyracks")
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
