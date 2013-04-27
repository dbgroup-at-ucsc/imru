package exp.imruVsSpark.data;

import java.io.File;
import java.io.PrintStream;
import java.util.Random;

import javax.script.CompiledScript;
import javax.script.ScriptEngineManager;

import edu.uci.ics.hyracks.imru.util.Rt;

public class DataGenerator {
    public static void generate(File templateDir, double numOfDataPoints, boolean hasLabel, File output)
            throws Exception {
        Random random = new Random();
        String formula = Rt.readFile(new File(templateDir, "dimensions.txt"));
        ScriptEngineManager manager = new ScriptEngineManager();
        com.sun.script.javascript.RhinoScriptEngine engine = (com.sun.script.javascript.RhinoScriptEngine) manager
                .getEngineByName("JavaScript");
        CompiledScript cs = engine.compile("var x=" + numOfDataPoints / 1000000.0 + ";" + formula);
        long dims = (long) ((Double) cs.eval() * 1000000.0);
        if (dims < 1000000)
            dims = 1000000;
        Rt.p("%,d", dims);
        Distribution dims_distribution = new Distribution(random, new File(templateDir, "non-empty-dims.txt"));
        Distribution value_distribution = new Distribution(random, new File(templateDir, "dim_value.txt"));
        PrintStream ps = new PrintStream(output);
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

    public static void main(String[] args) throws Exception {
        generate(new File("exp_data/product_name"), 51200, false, new File("/tmp/cache/productName.txt"));
    }
}
