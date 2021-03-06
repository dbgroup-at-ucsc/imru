package exp;

import java.io.File;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;

import edu.uci.ics.hyracks.ec2.HyracksNode;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.test0.GnuPlot;
import exp.types.ImruExperimentTimeoutException;

public class ClusterMonitor {
    DatagramSocket serverSocket;
    boolean exitFlag = false;
    public int nodes;
    public String templateIp;
    public String[] ip = new String[32];
    public int[] memory = new int[32];
    public float[] network = new float[32]; //MB
    public float[] cpu = new float[32];

    public ClusterMonitor() throws Exception {
        serverSocket = new DatagramSocket(6666);
        new Thread() {
            @Override
            public void run() {
                try {
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
                            String[] ss = s.split(" ");
                            String mac = ss[0];
                            int id = Integer.parseInt(mac.substring(mac
                                    .lastIndexOf(':') + 1));
                            if (id < 255) {
                                ip[id] = ss[1];
                                memory[id] = Integer.parseInt(ss[2]);
                                network[id] = Float.parseFloat(ss[3]);
                                cpu[id] = Float.parseFloat(ss[4]);
                                if (id >= nodes)
                                    nodes = id + 1;
                            } else {
                                templateIp = ss[1];
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                } catch (Exception e) {
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

    public void close() {
        exitFlag = true;
        serverSocket.close();
    }

    Thread monitorThread;
    boolean exit = false;

    public void start(final File resultDir, String name, HyracksNode[] nodes) {
        exit = false;
        final GnuPlot memory = new GnuPlot(resultDir, name + "mem", "time",
                "free (MB)");
        final GnuPlot cpu = new GnuPlot(resultDir, name + "cpu", "time",
                "usage (%)");
        final GnuPlot network = new GnuPlot(resultDir, name + "net", "time",
                "usage (MB)");
        final GnuPlot[] ps = new GnuPlot[] { memory, cpu, network };
        String[] ss = new String[nodes.length];
        //        ss[0] = "CC";
        for (int i = 0; i < nodes.length; i++)
            ss[i] = nodes[i].name;
        for (GnuPlot p : ps) {
            p.scale = false;
            p.colored = true;
            p.setPlotNames(ss);
        }
        monitorThread = new Thread() {
            @Override
            public void run() {
                try {
                    int id = 0;
                    long startTime = System.currentTimeMillis();
                    long nextTime = System.currentTimeMillis();
                    while (!exit) {
                        nextTime += 1000;
                        double time = (System.currentTimeMillis() - startTime) / 1000.0;
                        memory.startNewX(time);
                        cpu.startNewX(time);
                        network.startNewX(time);
                        for (int i = 0; i < ClusterMonitor.this.nodes; i++) {
                            memory.addY(ClusterMonitor.this.memory[i]);
                            cpu.addY(ClusterMonitor.this.cpu[i]);
                            network.addY(ClusterMonitor.this.network[i]);
                        }
                        if (nextTime > System.currentTimeMillis())
                            Thread.sleep(nextTime - System.currentTimeMillis());
                        id++;
                        if (id > 30) {
                            try {
                                for (GnuPlot p : ps) {
                                    p.finish();
                                    String cmd = "epstopdf --outfile="
                                            + new File(resultDir
                                                    .getParentFile(), p.name
                                                    + ".pdf").getAbsolutePath()
                                            + " "
                                            + new File(resultDir, p.name
                                                    + ".eps").getAbsolutePath();
                                    Rt.runAndShowCommand(cmd);
                                }
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            id = 0;
                        }
                    }
                } catch (InterruptedException e) {
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    for (GnuPlot p : ps) {
                        try {
                            p.finish();
                            String cmd = "epstopdf --outfile="
                                    + new File(resultDir.getParentFile(),
                                            p.name + ".pdf").getAbsolutePath()
                                    + " "
                                    + new File(resultDir, p.name + ".eps")
                                            .getAbsolutePath();
                            Rt.runAndShowCommand(cmd);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        };
        monitorThread.start();
    }

    public void stop() throws Exception {
        exit = true;
        monitorThread.join();
        //        monitorThread.interrupt();
    }

    public void waitTemplate(int timeout)
            throws ImruExperimentTimeoutException, InterruptedException {
        long startTime = System.currentTimeMillis();
        long maxTime = startTime + timeout;
        while (true) {
            if (System.currentTimeMillis() > maxTime)
                throw new ImruExperimentTimeoutException("timeout ");
            if (templateIp != null)
                return;
            Thread.sleep(500);
        }
    }

    public void waitIp(int n, int timeout)
            throws ImruExperimentTimeoutException, InterruptedException {
        int id = 0;
        long startTime = System.currentTimeMillis();
        long maxTime = startTime + timeout;
        int connected = 0;
        while (true) {
            if (System.currentTimeMillis() > maxTime)
                throw new ImruExperimentTimeoutException("timeout " + connected
                        + " of " + nodes + " connected");
            if (id % 10 == 0)
                Rt.p(nodes);
            connected = 0;
            boolean hasIp = true;
            for (int i = 0; i < nodes; i++) {
                if (ip[i] == null)
                    hasIp = false;
                else
                    connected++;
            }
            if (hasIp && nodes == n)
                return;
            Thread.sleep(500);
            id++;
        }
    }

    public static void main(String[] args) throws Exception {
        ClusterMonitor monitor = new ClusterMonitor();
        //                monitor.waitIp(8);
        Thread.sleep(2000);
        for (int i = 0; i < monitor.nodes; i++) {
            String p = monitor.ip[i] == null ? "" : monitor.ip[i]
                    .substring(monitor.ip[i].lastIndexOf('.') + 1);
            System.out.print(p + "\t");
        }
    }
}
