package exp.imruVsSpark.data;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Random;

import javax.script.CompiledScript;
import javax.script.ScriptEngineManager;

import edu.uci.ics.hyracks.imru.util.Rt;

public class DataGenerator {
    public static String TEMPLATE = "exp_data/product_name";
    public File templateDir;
    public double numOfDataPoints;
    public int dims;
    public Distribution dims_distribution;
    public Distribution value_distribution;
    Random random = new Random(1000);

    public DataGenerator(double numOfDataPoints, File templateDir)
            throws Exception {
        this.numOfDataPoints = numOfDataPoints;
        this.templateDir = templateDir;
        String formula = Rt.readFile(new File(templateDir, "dimensions.txt"));
        ScriptEngineManager manager = new ScriptEngineManager();
        com.sun.script.javascript.RhinoScriptEngine engine = (com.sun.script.javascript.RhinoScriptEngine) manager
                .getEngineByName("JavaScript");
        CompiledScript cs = engine.compile("var x=" + numOfDataPoints
                / 1000000.0 + ";" + formula);
        dims = (int) ((Double) cs.eval() * 1000000.0);
        if (dims < 1000000)
            dims = 1000000;
        Rt.p("%,d", dims);
        dims_distribution = new Distribution(random, new File(templateDir,
                "non-empty-dims.txt"));
        value_distribution = new Distribution(random, new File(templateDir,
                "dim_value.txt"));
    }

    public void generate(boolean hasLabel, File output) throws Exception {
        generate(hasLabel, new FileOutputStream(output));
    }

    public void generate(boolean hasLabel, OutputStream output)
            throws Exception {
        PrintStream ps = new PrintStream(new BufferedOutputStream(output,
                1024 * 1024));
        for (int i = 0; i < numOfDataPoints; i++) {
            int numOfDims = dims_distribution.get();
            if (hasLabel)
                ps.print(random.nextBoolean() ? 1 : 0 + " ");
            for (int j = 0; j < numOfDims; j++) {
                if (j > 0)
                    ps.print(" ");
                ps.print((long) (random.nextDouble() * dims));
                ps.print(":");
                ps.print(value_distribution.get());
            }
            ps.println();
        }
        ps.close();
    }

    public void generate(boolean hasLabel, int n, PrintStream ps, File infoFile)
            throws Exception {
        int numOfDims = dims_distribution.get();
        for (int i = 0; i < n; i++) {
            StringBuilder sb = new StringBuilder();
            if (hasLabel)
                sb.append(random.nextBoolean() ? 1 : 0 + " ");
            for (int j = 0; j < numOfDims; j++) {
                if (j > 0)
                    sb.append(" ");
                sb.append((long) (random.nextDouble() * dims));
                sb.append(":");
                sb.append(value_distribution.get());
            }
            ps.println(sb);
        }
        Rt.write(infoFile, ("" + numOfDims).getBytes());
    }

    public static void main(String[] args) throws Exception {
        int dataPoints = 1000000;
        DataGenerator d = new DataGenerator(dataPoints, new File(
                "exp_data/product_name"));

        long start = System.currentTimeMillis();
        if (args.length > 0)
            d.generate(false, new File(args[0]));
        else
            d.generate(false, new File("/data/b/data/imru/productName.txt"));
        Rt.p("%,d\t%,d", dataPoints, System.currentTimeMillis() - start);
    }
}
