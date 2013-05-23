package exp;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;

public class ClusterMonitor {
    public int nodes;
    public String[] ip = new String[32];
    public int[] memory = new int[32];

    public ClusterMonitor() throws Exception {
        new Thread() {
            @Override
            public void run() {
                try {
                    DatagramSocket serverSocket = new DatagramSocket(6666);
                    byte[] receiveData = new byte[1024];
                    while (true) {
                        try {
                            DatagramPacket receivePacket = new DatagramPacket(
                                    receiveData, receiveData.length);
                            serverSocket.receive(receivePacket);
                            byte[] bs = receivePacket.getData();
                            int len = receivePacket.getLength();
                            //                            Rt.p("RECEIVED: " + receivePacket.getAddress()
                            //                                    + " " + new String(bs, 0, len));
                            String s = new String(bs, 0, len);
                            String[] ss = s.split(" ");
                            String mac = ss[0];
                            int id = Integer.parseInt(mac.substring(mac
                                    .lastIndexOf(':') + 1));
                            ip[id] = ss[1];
                            memory[id] = Integer.parseInt(ss[2]);
                            if (id >= nodes)
                                nodes = id + 1;
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                } catch (SocketException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        if (false) {
            new Thread() {
                @Override
                public void run() {
                    try {
                        while (true) {
                            for (int i = 0; i < nodes; i++) {
                                String p = ip[i].substring(ip[i]
                                        .lastIndexOf('.') + 1);
                                System.out.print(p + "\t");
                            }
                            System.out.println();
                            for (int i = 0; i < nodes; i++) {
                                System.out.print(memory[i] + "\t");
                            }
                            System.out.println();
                            Thread.sleep(1000);
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }.start();
        }
    }

    public static void main(String[] args) throws Exception {
        new ClusterMonitor();
    }
}
