package exp.imruVsSpark;

import java.io.File;
import java.util.Stack;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.ec2.HyracksCluster;
import edu.uci.ics.hyracks.ec2.HyracksNode;
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
        int webport = 3000; //Spark didn't release the port for a long time
        File file = new File("/tmp/cache/sparkWebUI.txt");
        if (file.exists())
            webport = Integer.parseInt(Rt.readFile(file)) + 2;
        return webport;
    }

    public void startSpark() throws Exception {
        int webport = getSparkPort();
        File file = new File("/tmp/cache/sparkWebUI.txt");
        Rt.write(file, ("" + webport).getBytes());
        //rsync -vrultzCc  /home/wangrui/ucscImru/bin/exp/test0/ ubuntu@ec2-54-242-134-180.compute-1.amazonaws.com:"+home+"/test/bin/exp/test0/
        Rt.p("Starting spark");
        Rt.p("http://" + cluster.controller.publicIp + ":8080/");
        String master = cluster.controller.internalIp;
        String sshCmd = "ssh -l ubuntu -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no ";
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
        Rt.sleep(3000); //Must wait until UI start
        for (int i = 0; i < cluster.nodes.length; i++) {
            HyracksNode node = cluster.nodes[i];
            Rt.p(node.getName());
            String result = Rt.runCommand(sshCmd + node.internalIp
                    + " \"ps aux | grep spark\"");
            boolean hasWorker = result.contains("spark.deploy.worker.Worker");
            if (!hasWorker) {
                String cmd = home
                        + "/spark-0.7.0/run spark.deploy.worker.Worker -i "
                        + node.internalIp + " spark://" + master
                        + ":7077 --webui-port " + (webport + 1)
                        + "  > slaveSpark.log 2>&1 &";
                Rt.np(cmd);
                Rt.runCommand(sshCmd + node.internalIp + " \"" + cmd + "\"");
            }
        }
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
        Vector<Future> fs = new Vector<Future>();
        for (HyracksNode node : cluster.allNodes) {
            final HyracksNode node2 = node;
            Future<Integer> f = pool.submit(new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    try {
                        SSH ssh = node2.ssh();
                        ssh.verbose = false;
                        for (String line : ssh.execute("ps aux | grep spark",
                                true).split("\n")) {
                            if (line.contains("spark.deploy.master.Master")
                                    || line
                                            .contains("spark.deploy.worker.Worker")) {
                                String[] ss = line.split("[ |\t]+");
                                int pid = Integer.parseInt(ss[1]);
                                ssh.execute("kill " + pid);
                            }
                        }
                        for (String line : ssh.execute(
                                "ps aux | grep Xrunjdwp", true).split("\n")) {
                            if (line.contains("hyracks")) {
                                String[] ss = line.split("[ |\t]+");
                                int pid = Integer.parseInt(ss[1]);
                                ssh.execute("kill " + pid);
                            }
                        }
                        for (String line : ssh.execute("ps aux | grep java",
                                true).split("\n")) {
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
        Rt.p("All nodes stopped");
    }

    public void checkHyracks() throws Exception {
        HyracksConnection hcc = new HyracksConnection(
                cluster.controller.publicIp, 3099);
        for (int i = 0; i < 20; i++) {
            Rt.p(hcc.getNodeControllerInfos());
            if (hcc.getNodeControllerInfos().size() == cluster.nodes.length)
                return;
            Rt.p("Only " + hcc.getNodeControllerInfos().size() + " NC");
            Thread.sleep(1000);
        }
        cluster.printLogs(-1, 100);
        throw new Error("Only " + hcc.getNodeControllerInfos().size() + " NC");
    }
}
