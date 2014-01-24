package exp.imruVsSpark.lr.imru;

import java.io.File;

import edu.uci.ics.hyracks.imru.example.utils.Client;
import edu.uci.ics.hyracks.imru.util.CreateDeployment;
import edu.uci.ics.hyracks.imru.util.Rt;
import exp.imruVsSpark.data.DataGenerator;
import exp.types.ImruExpParameters;

public class ImruLRMain {
    static void run() throws Exception {
        String cmdline = "";
        cmdline += "-host localhost -port 3099 -debug -disable-logging";
        cmdline += " -input-paths " + LR.datafile.getAbsolutePath();
        int dims = LR.generateDataFile();
        ImruLRModel model = Client.run(new ImruLRJob(dims), new ImruLRModel(
                dims, LR.ITERATIONS), cmdline.split(" "));
        LR.verify(LR.loadData(LR.datafile), model.w);
    }

    public static int runVM(ImruExpParameters p) throws Exception {
        CreateDeployment.uploadJarFiles = false;
        DataGenerator.TEMPLATE = "/home/ubuntu/test/exp_data/product_name";
        if (!new File(DataGenerator.TEMPLATE).exists())
            DataGenerator.TEMPLATE = "/home/wangrui/test/exp_data/product_name";
        System.out.println("Connecting to " + Client.getLocalIp());
        File templateDir = new File(DataGenerator.TEMPLATE);
        DataGenerator dataGenerator = new DataGenerator(p.dataSize, templateDir);
        int dims = dataGenerator.dims;
        ImruLRModel model = Client.run(new ImruLRJob(dims), new ImruLRModel(
                dims, LR.ITERATIONS), p.getClientOptions());
        Rt.p("Total examples: " + model.totalExamples);
        return model.totalExamples;
    }

    public static void main(String[] args) throws Exception {
        run();
        System.exit(0);
    }
}
