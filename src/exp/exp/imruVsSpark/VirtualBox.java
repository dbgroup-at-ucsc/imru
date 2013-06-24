package exp.imruVsSpark;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * @author Rui Wang
 */
public class VirtualBox {
    // sudo ./VBoxLinuxAdditions.run --nox11
    static String VBoxManage(boolean show, String cmd) throws IOException {
        if (show) {
            return Rt
                    .runAndShowCommand("ssh 192.168.56.1 -p 6666 \"VBoxManage "
                            + cmd + "\"");
        } else {
            return Rt.runCommand("ssh 192.168.56.1 -p 6666 \"VBoxManage " + cmd
                    + "\"");
        }
    }

    static boolean hasMachine(String name) throws IOException {
        String result = VBoxManage(false, "list vms");
        return result.contains(name);
    }

    static boolean running(String name) throws IOException {
        String result = VBoxManage(false, "list runningvms");
        return result.contains(name);
    }

    public static void remove() throws Exception {
        for (int id = 0; id < 32; id++) {
            String machine = "imruexp_" + id;
            if (running(machine)) {
                Rt.p("poweroff " + machine);
                String cmd = "controlvm " + machine + " poweroff";
                VBoxManage(true, cmd);
            }
        }
        Thread.sleep(1000);
        for (int id = 0; id < 32; id++) {
            String machine = "imruexp_" + id;
            if (hasMachine(machine)) {
                Rt.p("removing " + machine);
                String cmd = "unregistervm " + machine + " --delete";
                VBoxManage(true, cmd);
            }
        }
    }

    public static void setup(int nodes, int memory, int cpuCap, int networkMB)
            throws Exception {
        Rt.p("Remove snapshot");
        for (int i = 0; i < 3; i++) {
            if (i == 2)
                throw new Error();
            String result = VBoxManage(true, "snapshot imru_template list");
            if (!result.contains("exp"))
                break;
            VBoxManage(true, "snapshot imru_template delete exp");
        }
        Rt.p("Take snapshot");
        VBoxManage(true, "snapshot imru_template take exp");
        for (int id = 0; id < nodes; id++) {
            String machine = "imruexp_" + id;
            String mac = String.format("080027b635%02x", id);
            if (!hasMachine(machine)) {
                Rt.p("cloning " + machine);
                String cmd = "clonevm imru_template --snapshot exp --options link,keepallmacs --name "
                        + machine + " --register";
                VBoxManage(true, cmd);
                VBoxManage(true, "modifyvm " + machine + " --memory " + memory
                        + " --cpus " + 1 + " --cpuexecutioncap " + cpuCap
                        + " --macaddress1 " + mac);
                if (networkMB > 0)
                    throw new Error(
                            "bandwidth control doesn't work in Virtualbox");
                //                    VBoxManage(true, "bandwidthctl " + machine
                //                            + " add net --type network --limit " + networkMB
                //                            + "M");
                //VBoxManage bandwidthctl creo --name net --add network --limit 100 
                //VBoxManage bandwidthctl creo --name net --delete
            }
        }
        for (int id = 0; id < nodes; id++) {
            String machine = "imruexp_" + id;
            if (!running(machine)) {
                Rt.p("starting " + machine);
                String cmd = "startvm " + machine + " --type headless";
                VBoxManage(true, cmd);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // remove();
        setup(8, 3000, 50, 0);
        // setup(16, 1500, 25);
    }
}
