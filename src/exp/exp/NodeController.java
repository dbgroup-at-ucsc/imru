package exp;

import java.io.IOException;
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

    static float cpu() throws IOException {
        Process process = Runtime.getRuntime().exec("mpstat");
        String[] lines = new String(Rt.read(process.getInputStream()))
                .split("\n");
        String[] ss = lines[lines.length - 1].trim().split(" +");
        Float idle = Float.parseFloat(ss[ss.length - 1]);
        return 100 - idle;
    }

    static int memory() throws IOException {
        Process process = Runtime.getRuntime().exec("free -m");
        String[] lines = new String(Rt.read(process.getInputStream()))
                .split("\n");
        if (lines.length < 2)
            return -1;
        String[] ss = lines[1].split(" +");
        return Integer.parseInt(ss[3]);
    }

    static long lastNetwork = -1;

    static float network() throws IOException {
        Process process = Runtime.getRuntime().exec("iptables -L -v -n");
        String[] lines = new String(Rt.read(process.getInputStream()))
                .split("\n");
        long bytes = 0;
        for (String line : lines) {
            if (line.startsWith("Chain INPUT")
                    || line.startsWith("Chain OUTPUT")) {
                //                Rt.p(line);
                line = line.substring(line.lastIndexOf(',') + 1).trim();
                line = line.substring(0, line.indexOf(' '));
                if (line.endsWith("K"))
                    bytes += 1024 * Long.parseLong(line.substring(0, line
                            .length() - 1));
                else if (line.endsWith("M"))
                    bytes += 1024 * 1024 * Long.parseLong(line.substring(0,
                            line.length() - 1));
                else if (line.endsWith("G"))
                    bytes += 1024 * 1024 * 1024 * Long.parseLong(line
                            .substring(0, line.length() - 1));
                else
                    bytes += Long.parseLong(line);
            }
        }
        if (lastNetwork < 0) {
            lastNetwork = bytes;
            return 0;
        }
        float usage = ((bytes - lastNetwork) / 1024f / 1024f);
        lastNetwork = bytes;
        return usage;
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
                            int mem = memory();
                            if (mem < 0)
                                continue;
                            float network = network();
                            float cpu = cpu();
                            String status = getIp("eth0") + " " + mem + " "
                                    + network + " " + cpu;
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
