package edu.uci.ics.hyracks.ec2;

import java.io.File;

public abstract class HyracksNode {
    public static String NAME_PREFIX = "NC";
    public static String HYRACKS_PATH = "/home/ubuntu/hyracks-ec2";
    HyracksCluster cluster;
    public int nodeId;
    public String name;
    public String publicIp;
    public String internalIp;

    public HyracksNode(HyracksCluster cluster, int nodeId, String publicIp,
            String internalIp, String name) {
        this.cluster = cluster;
        this.nodeId = nodeId;
        this.publicIp = publicIp;
        this.internalIp = internalIp;
        this.name = name;
    }

    public int getNodeId() {
        return nodeId;
    }

    public String getName() {
        return name;
    }

    abstract public SSH ssh() throws Exception;

    abstract public String ssh(String cmd) throws Exception;

    abstract public void rsync(SSH ssh, File localDir, String remoteDir)
            throws Exception;

    public void install(File hyracksEc2Root) throws Exception {
        SSH ssh = ssh();
        try {
            Rt.p("rync hyracks-ec2 to " + name);
            //            ssh.execute("sudo apt-get update");
            //            ssh.execute("sudo apt-get install openjdk-7-jre");
            rsync(ssh, hyracksEc2Root, HYRACKS_PATH);
            //            rsync(new File(imruRoot, "hyracks/hyracks-ec2/target/appassembler"),
            //                    "/home/ubuntu/fullstack_imru/hyracks/hyracks-ec2/target/appassembler");
            //            rsync(new File(imruRoot, "imru/imru-example/data"),
            //                    "/home/ubuntu/fullstack_imru/imru/imru-example/data");
            ssh.execute("chmod -R u+x " + HYRACKS_PATH + "/bin/*");
            ssh.execute("rm " + HYRACKS_PATH + "/lib/imru*");
            //            ssh.execute("chmod -R 755 /home/ubuntu/fullstack_imru/hyracks/hyracks-ec2/target/appassembler/bin/*");
            //            ssh.execute("chmod -R 755 /home/ubuntu/fullstack_imru/hyracks/hyracks-ec2/target/appassembler/bin/*");
            //            ec2.rsync(instance, ssh, hadoopRoot, "/home/ubuntu/hadoop-0.20.2");
            //            ssh.execute("chmod -R 755 /home/ubuntu/hadoop-0.20.2/bin/*");
        } finally {
            ssh.close();
        }
    }

    public void uploadData(String[] local, String[] remote) throws Exception {
        SSH ssh = ssh();
        try {
            for (int i = 0; i < local.length; i++) {
                Rt.p("rync " + local[i] + " to " + name + ":" + remote[i]);
                rsync(ssh, new File(local[i]), remote[i]);
            }
        } finally {
            ssh.close();
        }
    }

    public void startCC() throws Exception {
        SSH ssh = ssh();
        try {
            String result1 = ssh.execute("ps -ef|grep hyrackscc", true);
            if (result1.contains("java")) {
                Rt.p("CC is already running");
                return;
            }
            Rt.p("starting CC");
            ssh.execute("cd " + HYRACKS_PATH);
            ssh.execute("nohup bin/startccWithHostIp.sh " + internalIp);
            //        ec2.ssh(instance, "cd /home/ubuntu/fullstack_imru/hyracks/hyracks-ec2/target/appassembler;"
            //                + "bin/startccWithHostIp.sh " + instance.getPrivateIpAddress());
        } finally {
            ssh.close();
        }
    }

    public static boolean verbose = true;

    public void startNC() throws Exception {
        SSH ssh = ssh();
        try {
            ssh.verbose = verbose;
            String result1 = ssh.execute("ps -ef|grep hyracksnc", true);
            if (result1.contains("java")) {
                Rt.p(name + " is already running");
                return;
            }
            Rt.p("starting " + name);
            ssh.execute("cd " + HYRACKS_PATH);
            if (verbose)
                Rt.p("nohup bin/startncWithHostIpAndNodeId.sh "
                        + cluster.controller.internalIp + " " + internalIp
                        + " " + name);
            ssh.execute("nohup bin/startncWithHostIpAndNodeId.sh "
                    + cluster.controller.internalIp + " " + internalIp + " "
                    + name);
            //        ec2.ssh(instance, "cd /home/ubuntu/fullstack_imru/hyracks/hyracks-ec2/target/appassembler;"
            //                + "bin/startncWithHostIpAndNodeId.sh " + clusterControllerInstance.getPrivateIpAddress() + " "
            //                + internalIp + " " + nodeId);
        } finally {
            ssh.close();
        }
    }

    public void stopCC() throws Exception {
        SSH ssh = ssh();
        try {
            ssh.execute("cd " + HYRACKS_PATH);
            ssh.execute("bin/stopcc.sh");
        } finally {
            ssh.close();
        }
    }

    public void stopNC() throws Exception {
        SSH ssh = ssh();
        try {
            ssh.execute("cd " + HYRACKS_PATH);
            ssh.execute("bin/stopnc.sh");
        } finally {
            ssh.close();
        }
    }

    public void stopAll() throws Exception {
        SSH ssh = ssh();
        try {
            ssh.execute("cd " + HYRACKS_PATH);
            ssh.execute("bin/stopcc.sh");
            ssh.execute("bin/stopnc.sh");
        } finally {
            ssh.close();
        }
    }

    public void printProcesses() throws Exception {
        SSH ssh = ssh();
        try {
            if (nodeId == 0) {
                Rt.p("CC log:");
                ssh.execute("ps -ef|grep hyrackscc|grep java");
            }
            Rt.p(name + " log:");
            ssh.execute("ps -ef|grep hyracksnc|grep java", true);
        } finally {
            ssh.close();
        }
    }

    public void printLogs(int lines) throws Exception {
        try {
            SSH ssh = ssh();
            try {
                if (nodeId == -1) {
                    Rt.np("CC log:");
                    ssh.execute("tail -n " + lines + " /tmp/t1/logs/cc.log");
                }
                Rt.np(name + " log:");
                ssh.execute("tail -n " + lines + " /tmp/t2/logs/" + name
                        + ".log");
            } finally {
                ssh.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void printOutputs() throws Exception {
        SSH ssh = ssh();
        try {
            Rt.np(name + " output:");
            ssh.execute("tail -n 50 " + HYRACKS_PATH + "/nohup.out");
        } finally {
            ssh.close();
        }
    }

    public void listDir(String path) throws Exception {
        SSH ssh = ssh();
        try {
            Rt.np(name + ":" + path);
            ssh.execute("ll " + path);
        } finally {
            ssh.close();
        }
    }

    public void rmrDir(String path) throws Exception {
        SSH ssh = ssh();
        try {
            Rt.np(name + ":" + path);
            ssh.execute("rm -R " + path);
        } finally {
            ssh.close();
        }
    }

}
