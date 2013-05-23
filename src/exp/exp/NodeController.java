package exp;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

import edu.uci.ics.hyracks.imru.util.Rt;

public class NodeController {
    public static String getIp(String face) throws Exception {
        NetworkInterface ni = NetworkInterface.getByName(face);
        if (ni == null)
            return null;
        byte[] mac = ni.getHardwareAddress();
        Enumeration<InetAddress> addrs = ni.getInetAddresses();
        while (addrs.hasMoreElements()) {
            InetAddress inetAddress = (InetAddress) addrs.nextElement();
            byte[] bs = inetAddress.getAddress();
            if (bs.length == 16)
                continue;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < mac.length; i++) {
                if (i > 0)
                    sb.append(":");
                sb.append(mac[i] & 0xFF);
            }
            sb.append(" ");
            for (int i = 0; i < bs.length; i++) {
                if (i > 0)
                    sb.append(".");
                sb.append(bs[i] & 0xFF);
            }
            return sb.toString();
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        String host = "192.168.56.101";
        if (args.length > 0)
            host = args[0];
        final InetAddress address = InetAddress.getByName(host);
        new Thread("ipCheck") {
            public void run() {
                try {
                    DatagramSocket serverSocket = new DatagramSocket();
                    while (true) {
                        try {
                            Process process = Runtime.getRuntime().exec(
                                    "free -m");
                            String[] lines = new String(Rt.read(process
                                    .getInputStream())).split("\n");
                            if (lines.length < 2)
                                continue;
                            String[] ss = lines[1].split(" +");
                            int mem = Integer.parseInt(ss[3]);
                            String status = getIp("eth0") + " " + mem;
                            byte[] bs = status.getBytes();
                            DatagramPacket sendPacket = new DatagramPacket(bs,
                                    bs.length, address, 6666);
                            serverSocket.send(sendPacket);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        Thread.sleep(1000);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            };
        }.start();
    }
}
