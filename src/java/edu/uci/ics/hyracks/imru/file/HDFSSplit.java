package edu.uci.ics.hyracks.imru.file;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * Separate hadoop classes to avoid uploading
 * hadoop.jar when HDFS is not used.
 * Doesn't work as expected.
 * 
 * @author wangrui
 */
public class HDFSSplit implements Serializable {
    ConfigurationFactory confFactory;
    String path;
    String[] locations;
    long minSplitSize;
    long maxSplitSize;
    int splitIndex;

    public HDFSSplit(ConfigurationFactory confFactory, String path,
            long minSplitSize, long maxSplitSize, int splitIndex)
            throws IOException, InterruptedException {
        this.confFactory = confFactory;
        this.path = path;
        this.minSplitSize = minSplitSize;
        this.maxSplitSize = maxSplitSize;
        this.splitIndex = splitIndex;
    }

    public HDFSSplit(String path) throws IOException {
        this.path = path;
    }

    public String[] getLocations() throws IOException {
        if (locations == null) {
            // Use a dummy input format to create a list of
            // InputSplits for the input files
            Job dummyJob = new Job(confFactory.createConfiguration());
            TextInputFormat.addInputPaths(dummyJob, path);
            // Disable splitting of files:
            TextInputFormat.setMinInputSplitSize(dummyJob, minSplitSize);//Long.MAX_VALUE);
            TextInputFormat.setMaxInputSplitSize(dummyJob, maxSplitSize);//Long.MAX_VALUE);
            try {
                List<InputSplit> splits = new TextInputFormat()
                        .getSplits(dummyJob);
                locations = splits.get(splitIndex).getLocations();
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
        return locations;
    }

    public static List<IMRUFileSplit> get(ConfigurationFactory confFactory,
            String[] paths, long minSplitSize, long maxSplitSize)
            throws IOException, InterruptedException {
        Vector<IMRUFileSplit> list = new Vector<IMRUFileSplit>(paths.length);
        for (String path : paths) {
            // Use a dummy input format to create a list of
            // InputSplits for the input files
            Job dummyJob = new Job(confFactory.createConfiguration());
            TextInputFormat.addInputPaths(dummyJob, path);
            // Disable splitting of files:
            TextInputFormat.setMinInputSplitSize(dummyJob, minSplitSize);//Long.MAX_VALUE);
            TextInputFormat.setMaxInputSplitSize(dummyJob, maxSplitSize);//Long.MAX_VALUE);
            List<InputSplit> inputSplits = new TextInputFormat()
                    .getSplits(dummyJob);
            for (int i = 0; i < inputSplits.size(); i++) {
                list.add(new IMRUFileSplit(new HDFSSplit(confFactory, path,
                        minSplitSize, maxSplitSize, i)));
            }
        }
        return list;
    }

    public InputStream getInputStream() throws IOException {
        Path path = new Path(this.path);
        Configuration conf = confFactory.createConfiguration();
        FileSystem dfs = FileSystem.get(conf);
        return HDFSUtils.open(dfs, conf, path);
    }

    public boolean isDirectory() throws IOException {
        Path path = new Path(this.path);
        Configuration conf = confFactory.createConfiguration();
        FileSystem dfs = FileSystem.get(conf);
        return dfs.isDirectory(path);
    }
}
