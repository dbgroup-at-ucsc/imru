package exp.test0.imruTest;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import edu.uci.ics.hyracks.imru.file.ConfigurationFactory;
import edu.uci.ics.hyracks.imru.file.HDFSSplit;
import edu.uci.ics.hyracks.imru.file.IMRUFileSplit;
import edu.uci.ics.hyracks.imru.util.Rt;

public class TestHdfs {
    public static void main(String[] args) throws Exception {
        Configuration hadoopConf = new Configuration();
        ConfigurationFactory confFactory = new ConfigurationFactory("");
        List<IMRUFileSplit> splits = HDFSSplit.get(confFactory,
                new String[] { "/home/wangrui/a/imru/install.sh" },
                Long.MAX_VALUE, Long.MAX_VALUE);
        for (IMRUFileSplit s : splits) {
            Rt.p(s.getLocations().length);
            Rt.p("split");
        }
    }
}
