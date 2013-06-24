package exp.test0;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;

import edu.uci.ics.hyracks.ec2.HyracksCluster;
import edu.uci.ics.hyracks.ec2.HyracksNode;
import edu.uci.ics.hyracks.ec2.SSH;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.ClusterMonitor;
import exp.imruVsSpark.LocalCluster;
import exp.imruVsSpark.VirtualBox;

public class NetworkSpeedTest {
    static ClusterMonitor monitor;

    static void host(String ip) throws Exception {
        ServerSocket socket = new ServerSocket();
        socket.bind(new InetSocketAddress(InetAddress.getByName(ip), 2000));
        Rt.p("Bind " + ip);
        while (true) {
            Socket s = socket.accept();
            InputStream in = s.getInputStream();
            OutputStream out = s.getOutputStream();
            byte[] bs = new byte[1024 * 1024];
            while (true) {
                int len = in.read(bs);
                if (len < 0)
                    break;
                out.write(bs, 0, len);
            }
            break;
        }
    }

    static void client(String ip) throws Exception {
        Rt.p("connecting " + ip);
        Socket s = new Socket(ip, 2000);
        InputStream in = s.getInputStream();
        OutputStream out = s.getOutputStream();
        byte[] bs = new byte[16 * 1024];
        Rt.p("start");
        long start = System.currentTimeMillis();
        long transfered = 0;
        for (int i = 0; i < 10000; i++) {
            out.write(bs);
            transfered += bs.length;
            int left = bs.length;
            while (left > 0) {
                int len = in.read(bs);
                transfered += len;
                if (len < 0)
                    break;
                left -= len;
            }
            if (i % 1 == 0) {
                long time = System.currentTimeMillis() - start;
                Rt.p("time=%.2fs speed=%.2fMB/s", time / 1000.0, transfered
                        / (time / 1000.0) / 1024 / 1024);
            }
        }
        long time = System.currentTimeMillis() - start;
        Rt.p("time=%.2fs speed=%.2fMB/s", time / 1000.0, transfered
                / (time / 1000.0) / 1024 / 1024);
        s.close();
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0) {
            if ("host".equals(args[0]))
                host(args[1]);
            else
                client(args[1]);
            return;
        }
                VirtualBox.remove();
                System.exit(0);
                VirtualBox.setup(2, 2000, 50, 0);
        monitor = new ClusterMonitor();
        String[] nodes = new String[2];
        monitor.waitIp(nodes.length);
        for (int i = 0; i < nodes.length; i++)
            nodes[i] = monitor.ip[i];
        String userName = "ubuntu";
        File home = new File(System.getProperty("user.home"));
        final LocalCluster cluster = new LocalCluster(new HyracksCluster(
                nodes[0], nodes, userName, new File(home, ".ssh/id_rsa")),
                userName);
        for (HyracksNode node : cluster.cluster.nodes) {
            SSH ssh = node.ssh();
            node.rsync(ssh, new File("/home/wangrui/ucscImru/bin"), "/home/"
                    + cluster.user + "/test/bin/");
        }
        cluster.setNetworkSpeed(0);
        new Thread() {
            public void run() {
                try {
                    SSH ssh = cluster.cluster.nodes[0].ssh();
                    ssh.execute("cd test;");
                    ssh.execute("sh st.sh " + NetworkSpeedTest.class.getName()
                            + " host " + cluster.cluster.nodes[0].internalIp);
                    ssh.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            };
        }.start();
        new Thread() {
            public void run() {
                try {
                    Thread.sleep(5000);
                    SSH ssh = cluster.cluster.nodes[1].ssh();
                    ssh.execute("cd test;");
                    ssh.execute("sh st.sh " + NetworkSpeedTest.class.getName()
                            + " client " + cluster.cluster.nodes[0].internalIp);
                    ssh.close();
                    System.exit(0);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            };
        }.start();

        //4K 4.45MB
        //64K 34.40MB
        //256K 55.68MB

        //New version:
        //256K 76.37MB

        //        System.exit(0);
    }
}
