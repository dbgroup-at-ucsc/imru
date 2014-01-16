package exp.imruVsSpark.kmeans;

import edu.uci.ics.hyracks.imru.example.helloworld.HelloWorldJob;
import edu.uci.ics.hyracks.imru.example.utils.Client;

public class Test {

    /**
     * @param args
     */
    public static void main(String[] args) {
        String cmd = "-host 192.168.56.104 -port 3099 -agg-tree-type nary -agg-count 2 -fan-in 2 -mem-cache -input-paths NC0:/data/size1/nodes2/imru0.txt,NC1:/data/size1/nodes2/imru1.txt";
        try {
            String finalModel = Client.run(new HelloWorldJob(), "", cmd
                    .split(" "));
            System.out.println("FinalModel: " + finalModel);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(0);
        }
        System.exit(0);
    }

}
