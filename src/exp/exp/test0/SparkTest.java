package exp.test0;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import edu.uci.ics.hyracks.imru.example.utils.CreateHar;
import edu.uci.ics.hyracks.imru.util.Rt;
import spark.api.java.*;
import spark.api.java.function.Function;

public class SparkTest {
    public static void main(String[] args) throws IOException {
        ByteArrayOutputStream memory = new ByteArrayOutputStream();
        ZipOutputStream zip2 = new ZipOutputStream(memory);
        CreateHar.add("", new File("bin"), zip2);
        zip2.finish();
        Rt.write(new File("tmp/simple-project-1.0.jar"), memory.toByteArray());

        String logFile = "/var/log/syslog"; // Should be some file on your system
        JavaSparkContext sc = new JavaSparkContext("local", "Simple Job", "lib/spark-0.7.0",
                new String[] { "tmp/simple-project-1.0.jar" });
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        long numAs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.contains("a");
            }
        }).count();

        long numBs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.contains("b");
            }
        }).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    }
}
