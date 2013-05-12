package exp.test0;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import edu.uci.ics.hyracks.imru.util.Rt;

import spark.api.java.JavaRDD;
import spark.api.java.JavaSparkContext;
import spark.api.java.function.Function;
import spark.api.java.function.Function2;

public class SparkDebug {
    public static void copy(InputStream in, OutputStream out) throws IOException {
        byte[] bs = new byte[1024];
        while (true) {
            int len = in.read(bs);
            if (len <= 0)
                break;
            out.write(bs, 0, len);
        }
    }

    public static void copy(File file, OutputStream out) throws IOException {
        FileInputStream input = new FileInputStream(file);
        copy(input, out);
        input.close();
    }

    public static void add(String name, File file, ZipOutputStream zip) throws IOException {
        if (file.isDirectory()) {
            for (File f : file.listFiles())
                add(name.length() == 0 ? f.getName() : name + "/" + f.getName(), f, zip);
        } else if (name.length() > 0) {
            if ("imru-deployment.properties".equals(name))
                return;
            ZipEntry entry = new ZipEntry(name);
            entry.setTime(file.lastModified());
            zip.putNextEntry(entry);
            copy(file, zip);
            zip.closeEntry();
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("spark.akka.frameSize", "512");
        ByteArrayOutputStream memory = new ByteArrayOutputStream();
        ZipOutputStream zip2 = new ZipOutputStream(memory);
        add("", new File("bin"), zip2);
        zip2.finish();
        FileOutputStream fileOutputStream = new FileOutputStream(new File("/tmp/simple-project-1.0.jar"));
        fileOutputStream.write(memory.toByteArray());
        fileOutputStream.close();
        fileOutputStream = new FileOutputStream(new File("/tmp/tmp.txt"));
        fileOutputStream.write("s1\ns2\ns3\ns4\ns5".getBytes());
        fileOutputStream.close();

        String master = "spark://" + args[0] + ":7077";
        System.out.println(master);
        System.out.println(master);
        JavaSparkContext sc = new JavaSparkContext(master, "SparkDebug", "/home/ubuntu/spark-0.7.0",
                new String[] { "/tmp/simple-project-1.0.jar" });

        JavaRDD<String> lines = sc.textFile("/tmp/tmp.txt",2);

        String c = lines.map(new Function<String, String>() {
            @Override
            public String call(String a) throws Exception {
                return a;
            }
        }).reduce(new Function2<String, String, String>() {
            @Override
            public String call(String a, String b) throws Exception {
                return a + b;
            }
        });
        System.out.println(c);
    }
}
