package exp.imruVsSpark.kmeans;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;

import edu.uci.ics.hyracks.imru.util.Rt;

public class RunExp {
    static long lastResponseTime;

    public static void showInputStream(final InputStream is) {
        new Thread() {
            @Override
            public void run() {
                byte[] bs = new byte[1024];
                try {
                    while (true) {
                        int len = is.read(bs);
                        if (len < 0)
                            break;
                        String s = new String(bs, 0, len);
                        System.out.print(s);
                        lastResponseTime = System.currentTimeMillis();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }.start();
    }

    static Process process;
    static boolean succeed = true;

    public static void main(String[] args) throws Exception {
        new Thread() {
            public void run() {
                try {
                    while (true) {
                        if (System.currentTimeMillis() - lastResponseTime > 20 * 60 * 1000L) {
                            succeed = false;
                            process.destroy();
                        }
                        Thread.sleep(1000);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();
        while (true) {
            succeed = true;
            Rt.p("starting process");
            lastResponseTime = System.currentTimeMillis();
            process = Runtime.getRuntime().exec("sh exp.sh");
            showInputStream(process.getInputStream());
            showInputStream(process.getErrorStream());

            try {
                process.waitFor();
                if (succeed)
                    break;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Rt.p("ended process");
        }
    }
}
