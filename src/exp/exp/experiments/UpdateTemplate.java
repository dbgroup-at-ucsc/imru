package exp.experiments;

import java.io.File;

import edu.uci.ics.hyracks.ec2.SSH;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.ClusterMonitor;
import exp.VirtualBoxExperiments;
import exp.imruVsSpark.VirtualBox;

public class UpdateTemplate {
    public static void main(String[] args) throws Exception {
        Rt.runAndShowCommand("mvn package -DskipTests=true", new File(
                "/data/a/imru/hyracks10/hyracks"));
        Rt.p(Rt.lastResult);
        if (Rt.lastResult!=0)
            throw new Error();
        ClusterMonitor monitor = new ClusterMonitor();
        VirtualBox.remove();
        VirtualBox.startTemplate();
        monitor.waitTemplate(30000);

        String ip = monitor.templateIp;
        VirtualBoxExperiments.createTemplate(ip, "ubuntu");
        SSH ssh = new SSH("ubuntu", ip, 22, new File(
                "/home/wangrui/.ssh/id_rsa"));
        ssh.timeout = 1000;
        ssh.execute("sudo poweroff", false);
        ssh.close();
        System.exit(0);
    }
}
