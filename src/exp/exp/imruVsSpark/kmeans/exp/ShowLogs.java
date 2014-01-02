package exp.imruVsSpark.kmeans.exp;

import java.io.File;

import edu.uci.ics.hyracks.ec2.HyracksCluster;
import exp.imruVsSpark.LocalCluster;

public class ShowLogs {
    public static void main(String[] args) throws Exception {
        String[] nodes = ("NC0: 192.168.56.103\n" + "NC1: 192.168.56.105\n"
                + "NC2: 192.168.56.106\n" + "NC3: 192.168.56.107\n"
                + "NC4: 192.168.56.110\n" + "NC5: 192.168.56.104\n"
                + "NC6: 192.168.56.108\n" + "NC7: 192.168.56.109").split("\n");
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = nodes[i].substring(nodes[i].indexOf(':') + 1).trim();
        }
        File home = new File(System.getProperty("user.home"));
        String userName = "ubuntu";
        LocalCluster cluster = new LocalCluster(new HyracksCluster(nodes[0],
                nodes, userName, new File(home, ".ssh/id_rsa")), userName);
        cluster.cluster.printLogs(-1, 1000);
    }
}
