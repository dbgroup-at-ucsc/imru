package hyracksTest;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import edu.uci.ics.hyracks.imru.util.Rt;

public class Test {
    public static void main(String[] args) throws IOException {
        for (int i = 0; i < 52; i++) {
            FileOutputStream out = new FileOutputStream(new File(
                    "/home/wangrui/fullstack_imru/imru/imru-example/data/helloworld/hello" + i + ".txt"));
            char c=(char)('a'+i);
            if (i>=26)
                c=(char)('A'+i-26);
            out.write((""+c).getBytes());
            out.close();
            System.out.print(c);
        }
        Properties p = System.getProperties();
        //       for (Object key : p.keySet()) {
        //           R.p(key+" "+p.getProperty((String)key));
        //       }
        String string = System.getProperty("java.class.path");
        for (String s : string.split(File.pathSeparator)) {
//            R.p(s);
        }
    }
}
