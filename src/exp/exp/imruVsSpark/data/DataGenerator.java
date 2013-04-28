package exp.imruVsSpark.data;

import java.io.File;
import java.io.PrintStream;
import java.util.Random;

import javax.script.CompiledScript;
import javax.script.ScriptEngineManager;

import edu.uci.ics.hyracks.imru.util.Rt;

public class DataGenerator {
    public static int DEBUG_DATA_POINTS = 100;
    public static int DEBUG_K = 3;
    public static int DEBUG_ITERATIONS = 5;
    public File templateDir;
    public double numOfDataPoints;
    public int dims;
    public Distribution dims_distribution;
    public Distribution value_distribution;
    Random random = new Random();

    public DataGenerator(double numOfDataPoints, File templateDir) throws Exception {
        this.numOfDataPoints = numOfDataPoints;
        this.templateDir = templateDir;
        String formula = Rt.readFile(new File(templateDir, "dimensions.txt"));
        ScriptEngineManager manager = new ScriptEngineManager();
        com.sun.script.javascript.RhinoScriptEngine engine = (com.sun.script.javascript.RhinoScriptEngine) manager
                .getEngineByName("JavaScript");
        CompiledScript cs = engine.compile("var x=" + numOfDataPoints / 1000000.0 + ";" + formula);
        dims = (int) ((Double) cs.eval() * 1000000.0);
        if (dims < 1000000)
            dims = 1000000;
        Rt.p("%,d", dims);
        dims_distribution = new Distribution(random, new File(templateDir, "non-empty-dims.txt"));
        value_distribution = new Distribution(random, new File(templateDir, "dim_value.txt"));
    }

    public void generate(boolean hasLabel, File output) throws Exception {
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
        DataGenerator d = new DataGenerator(DEBUG_DATA_POINTS, new File("exp_data/product_name"));
        d.generate(false, new File("/data/b/data/imru/productName.txt"));
    }
}
