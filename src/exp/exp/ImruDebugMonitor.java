package exp;

import java.io.PrintStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;

import edu.uci.ics.hyracks.imru.util.Rt;

public class ImruDebugMonitor {
    DatagramSocket serverSocket;
    PrintStream ps;
    boolean exitFlag = false;

    public ImruDebugMonitor(String path) throws Exception {
        serverSocket = new DatagramSocket(6667);
        ps=new PrintStream(path);
        new Thread() {
            @Override
            public void run() {
                try {
                    long start=System.currentTimeMillis();
                    byte[] receiveData = new byte[1024];
                    while (!exitFlag) {
                        try {
                            DatagramPacket receivePacket = new DatagramPacket(
                                    receiveData, receiveData.length);
                            serverSocket.receive(receivePacket);
                            byte[] bs = receivePacket.getData();
                            int len = receivePacket.getLength();
                            //                            Rt.p("RECEIVED: " + receivePacket.getAddress()
                            //                                    + " " + new String(bs, 0, len));
                            String s = new String(bs, 0, len);
                            ps.println((System.currentTimeMillis()-start)+"\t"+s);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();
    }

    public void close() {
        exitFlag = true;
        serverSocket.close();
        ps.close();
    }
}
