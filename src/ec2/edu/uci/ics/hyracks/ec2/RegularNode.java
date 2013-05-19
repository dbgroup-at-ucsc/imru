package edu.uci.ics.hyracks.ec2;

import java.io.File;

public class RegularNode extends HyracksNode {
    String user;
    File permFile;

    public RegularNode(HyracksCluster cluster, int nodeId, String publicIp,
            String internalIp, String name, String user, File permFile) {
        super(cluster, nodeId, publicIp, internalIp, name);
        this.user = user;
        this.permFile = permFile;
    }

    @Override
    public SSH ssh() throws Exception {
        return new SSH(user, publicIp, 22, permFile);
    }

    @Override
    public String ssh(String cmd) throws Exception {
        return Rt.runAndShowCommand("ssh", "-i", permFile.getAbsolutePath(),
                publicIp, "-l", user, cmd);
    }

    @Override
    public void rsync(SSH ssh, File localDir, String remoteDir)
            throws Exception {
        String rsync = "/usr/bin/rsync";
        if (new File(rsync).exists()) {
            //            grantAccessToLocalMachine(instance);
            if (localDir.isDirectory()) {
                if (!remoteDir.endsWith("/"))
                    remoteDir += "/";
                ssh.execute("if test ! \"(\" -e '" + remoteDir
                        + "' \")\";then mkdir -p '" + remoteDir + "';fi;");
                Rt
                        .runAndShowCommand(
                                rsync,
                                "--rsh=ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no",
                                "-vrultzCc", localDir.getAbsolutePath() + "/",
                                user + "@" + publicIp + ":" + remoteDir);
            } else {
                String s = new File(remoteDir).getParent();
                ssh.execute("if test ! \"(\" -e '" + s
                        + "' \")\";then mkdir -p '" + s + "';fi;");
                Rt
                        .runAndShowCommand(
                                rsync,
                                "--rsh=ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no",
                                "-vrultzCc", localDir.getAbsolutePath(), user
                                        + "@" + publicIp + ":" + remoteDir);
            }
        } else {
            throw new Exception("Please install rsync");
        }
    }
}
