package exp.imruVsSpark.kmeans.exp;

import java.io.File;

import edu.uci.ics.hyracks.ec2.Rt;
import edu.uci.ics.hyracks.ec2.SSH;
import exp.imruVsSpark.VirtualBox;
import exp.imruVsSpark.kmeans.VirtualBoxExperiments;

public class UpdateTemplate {
    public static void main(String[] args) throws Exception {
        //        VirtualBoxExperiments.createTemplate("192.168.56.102", "ubuntu");
        //        System.exit(0);
        VirtualBox.remove();
        String ip = "192.168.56.110";
        VirtualBox.startTemplate();
        Rt.sleep(5000);
        Rt.runAndShowCommand("mvn package -DskipTests=true", new File(
                "/data/a/imru/hyracks10/hyracks"));
        VirtualBoxExperiments.createTemplate(ip, "ubuntu");
        SSH ssh = new SSH("ubuntu", ip, 22, new File(
                "/home/wangrui/.ssh/id_rsa"));
        ssh.timeout = 1000;
        ssh.execute("sudo poweroff", false);
        ssh.close();
    }
}
