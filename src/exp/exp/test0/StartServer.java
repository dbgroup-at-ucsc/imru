package exp.test0;

import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.common.controllers.CCConfig;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.imru.util.Client;

public class StartServer {
    public static void main(String[] args) throws Exception {
        //start cluster controller
        CCConfig ccConfig = new CCConfig();
        ccConfig.clientNetIpAddress = "127.0.0.1";
        ccConfig.clusterNetIpAddress = "127.0.0.1";
        ccConfig.clusterNetPort = 1099;
        ccConfig.clientNetPort = 3099;
        ccConfig.defaultMaxJobAttempts = 0;
        ccConfig.jobHistorySize = 10;
        ccConfig.appCCMainClass = "edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUCCBootstrapImpl";

        //start node controller
        ClusterControllerService cc = new ClusterControllerService(ccConfig);
        cc.start();

        for (int i = 0; i < 2; i++) {
            NCConfig config = new NCConfig();
            config.nodeId = "NC" + i;
            config.ccHost = "127.0.0.1";
            config.ccPort = 1099;
            config.clusterNetIPAddress = "127.0.0.1";
            config.dataIPAddress = "127.0.0.1";
            config.datasetIPAddress = "127.0.0.1";
            config.appNCMainClass = "edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUNCBootstrapImpl";
            NodeControllerService nc = new NodeControllerService(config);
            nc.start();
        }
//        Client.disableLogging();
    }
}
