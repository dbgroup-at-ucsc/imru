package edu.uci.ics.hyracks.imru.example.utils;

import java.io.File;
import java.io.Serializable;

import edu.uci.ics.hyracks.imru.trainmerge.TrainMergeJob;

public class ImruEC2TrainMerge extends ImruEC2 {
    public ImruEC2TrainMerge(File credentialsFile, File privateKey)
            throws Exception {
        super(credentialsFile, privateKey);
    }

    public <M extends Serializable, D extends Serializable, R extends Serializable> M run(
            TrainMergeJob<M> job, M initialModel, String appName, String paths)
            throws Exception {
        //        cluster.printLogs(-1);
        init();
        String cmdline = "";
        cmdline += "-host " + ccHostName;
        cmdline += " -port 3099";
        cmdline += " -app " + appName;
        cmdline += " -input-paths " + paths;
        cmdline += " -model-file-name helloworld";
        cmdline = cmdline.trim();
        System.out.println("Using command line: " + cmdline);
        String[] args = cmdline.split(" ");
        return ClientTrainMerge.run(job, initialModel, args);
    }
}
