package exp.imruVsSpark;

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

    public void startSpark() throws Exception {
        //rsync -vrultzCc  /home/wangrui/ucscImru/bin/exp/test0/ ubuntu@ec2-54-242-134-180.compute-1.amazonaws.com:"+home+"/test/bin/exp/test0/
        //screen -d -m -S m "+home+"/spark-0.7.0/run spark.deploy.master.Master -i ${IP} -p 7077
        //screen -d -m -S s "+home+"/spark-0.7.0/run spark.deploy.worker.Worker spark://${IP}:7077
        Rt.p("Starting spark");
        Rt.p("http://" + cluster.controller.publicIp + ":8080/");
        String master = cluster.controller.internalIp;
        {
            HyracksNode node = cluster.controller;
            Rt.p(node.getName());
            SSH ssh = node.ssh();
            String result = ssh.execute("ps aux | grep spark", true);
            boolean hasMaster = result.contains("spark.deploy.master.Master");
            if (!hasMaster) {
                String cmd = "bash -c \"" + home
                        + "/spark-0.7.0/run spark.deploy.master.Master -i "
                        + master + " -p 7077 > masterSpark.log 2>&1 &\"";
                Rt.np(cmd);
                ssh.execute(cmd);
                ssh.execute("sleep 2s");
                ssh.execute("tail -n 100 " + home + "/masterSpark.log");
                ssh.close();
            }
        }
        for (int i = 0; i < cluster.nodes.length; i++) {
            HyracksNode node = cluster.nodes[i];
            Rt.p(node.getName());
            SSH ssh = node.ssh();
            String result = ssh.execute("ps aux | grep spark", true);
            boolean hasWorker = result.contains("spark.deploy.worker.Worker");

            if (!hasWorker) {
                String cmd = "bash -c \"" + home
                        + "/spark-0.7.0/run spark.deploy.worker.Worker -i "
                        + node.internalIp + " spark://" + master
                        + ":7077  > slaveSpark.log 2>&1 &\"";
                Rt.np(cmd);
                ssh.execute(cmd);
                ssh.execute("sleep 2s");
            }
            ssh.execute("tail -n 100 " + home + "/slaveSpark.log");
            ssh.close();
        }
        Thread.sleep(2000);
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

    public void stopAll() throws Exception {
        for (HyracksNode node : cluster.allNodes) {
            SSH ssh = node.ssh();
            for (String line : ssh.execute("ps aux | grep spark", true).split(
                    "\n")) {
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
            for (String line : ssh.execute("ps aux | grep java", true).split(
                    "\n")) {
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
            //            ssh.execute("sudo chmod ugo+rw /mnt -R");
            ssh.execute("free -m");
            ssh.close();
        }
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
