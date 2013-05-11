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

import spark.api.java.JavaRDD;
import spark.api.java.JavaSparkContext;
import spark.api.java.function.Function;
import spark.api.java.function.Function2;

public class SparkDebug {
    public static void copy(InputStream in, OutputStream out)
            throws IOException {
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

    public static void add(String name, File file, ZipOutputStream zip)
            throws IOException {
        if (file.isDirectory()) {
            for (File f : file.listFiles())
                add(
                        name.length() == 0 ? f.getName() : name + "/"
                                + f.getName(), f, zip);
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
//        System.setProperty("spark.akka.frameSize", "100");
        ByteArrayOutputStream memory = new ByteArrayOutputStream();
        ZipOutputStream zip2 = new ZipOutputStream(memory);
        add("", new File("bin"), zip2);
        zip2.finish();
        FileOutputStream fileOutputStream = new FileOutputStream(new File(
                "/tmp/simple-project-1.0.jar"));
        fileOutputStream.write(memory.toByteArray());
        fileOutputStream.close();
        fileOutputStream = new FileOutputStream(new File("/tmp/tmp.txt"));
        fileOutputStream.write("abc".getBytes());
        fileOutputStream.close();

        String master = "spark://192.168.56.101:7077";
        JavaSparkContext sc = new JavaSparkContext(master, "SparkDebug",
                "/home/wangrui/b/soft/lib/spark-0.7.0",
                new String[] { "/tmp/simple-project-1.0.jar" });

        JavaRDD<String> lines = sc.textFile("/tmp/tmp.txt");

        byte[] bs = lines.map(new Function<String, byte[]>() {
            @Override
            public byte[] call(String arg0) throws Exception {
                return new byte[10 * 1024 * 1024];
            }
        }).reduce(new Function2<byte[], byte[], byte[]>() {
            @Override
            public byte[] call(byte[] arg0, byte[] arg1) throws Exception {
                return new byte[0];
            }
        });

        System.out.println(bs.length);
    }
}
