package edu.uci.ics.hyracks.imru.file;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * Separate hadoop classes to avoid uploading
 * hadoop.jar when HDFS is not used.
 * Doesn't work as expected.
 * 
 * @author wangrui
 */
public class HDFSSplit implements Serializable {
    public static class BoundInputStream extends InputStream {
        InputStream in;
        long left;

        public BoundInputStream(InputStream in, long start, long length)
                throws IOException {
            this.in = in;
            in.skip(start);
            left = length;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (left == 0)
                return -1;
            if (len < left) {
                left -= len;
            } else {
                len = (int) left;
                left = 0;
            }
            return in.read(b, off, len);
        }

        @Override
        public int read() throws IOException {
            if (left == 0)
                return -1;
            left--;
            return in.read();
        }
    }

    ConfigurationFactory confFactory;
    String path;
    long startOffset;
    long length;
    String[] hosts;
    Hashtable<String, String> params;

    public HDFSSplit(ConfigurationFactory confFactory, String path,
            long startOffset, long length, String[] hosts,
            Hashtable<String, String> params) throws IOException,
            InterruptedException {
        this.confFactory = confFactory;
        this.path = path;
        this.startOffset = startOffset;
        this.length = length;
        this.hosts = hosts;
        this.params = params;
    }

    public String getPath() {
        return path;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public long getLength() {
        return length;
    }

    public String[] getHosts() {
        return hosts;
    }

    public String getParameter(String key) {
        if (params == null)
            return null;
        return params.get(key);
    }

    public FileSplit getSplit() throws HyracksDataException, IOException {
        // Use a dummy input format to create a list of
        // InputSplits for the input files
        return new FileSplit(new Path(this.path), startOffset, length, hosts);
    }

    public org.apache.hadoop.mapred.FileSplit getMapredSplit()
            throws HyracksDataException, IOException {
        // Use a dummy input format to create a list of
        // InputSplits for the input files
        return new org.apache.hadoop.mapred.FileSplit(new Path(this.path),
                startOffset, length, hosts);
    }

    public static List<HDFSSplit> get(ConfigurationFactory confFactory,
            String[] paths, int requestedSplits, long minSplitSize,
            long maxSplitSize) throws IOException, InterruptedException {
        Vector<HDFSSplit> list = new Vector<HDFSSplit>(paths.length);
        for (String path : paths) {
            Hashtable<String, String> params = null;
            int t = path.indexOf('?');
            if (t >= 0) {
                params = new Hashtable<String, String>();
                for (String s : path.substring(t + 1).split("&")) {
                    String[] kv = s.split("=", 2);
                    params.put(kv[0], kv[1]);
                }
                path = path.substring(0, t);
            }
            if (paths.length == 1 && requestedSplits > 1) {
                Configuration conf = confFactory.createConfiguration();
                FileSystem dfs = FileSystem.get(conf);
                long length = dfs.getFileStatus(new Path(path)).getLen();
                minSplitSize = (int) Math
                        .ceil((float) length / requestedSplits);
                maxSplitSize = (int) Math
                        .ceil((float) length / requestedSplits);
            }
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
                InputSplit inputSplit = (InputSplit) inputSplits.get(i);
                if (inputSplit instanceof org.apache.hadoop.mapreduce.lib.input.FileSplit) {
                    org.apache.hadoop.mapreduce.lib.input.FileSplit fileSplit = (org.apache.hadoop.mapreduce.lib.input.FileSplit) inputSplit;
                    list.add(new HDFSSplit(confFactory, path, fileSplit
                            .getStart(), fileSplit.getLength(), fileSplit
                            .getLocations(), params));
                } else {
                    throw new IOException("Can't handle "
                            + inputSplit.getClass().getName());
                }
            }
        }
        if (paths.length == 1 && requestedSplits > 1
                && list.size() != requestedSplits)
            throw new Error();
        return list;
    }

    public InputStream getInputStream() throws IOException {
        Configuration conf = confFactory.createConfiguration();
        FileSystem dfs = FileSystem.get(conf);
        InputStream is = HDFSUtils.open(dfs, conf, new Path(this.path));;
        return new BoundInputStream(is, this.startOffset, this.length);
    }

    public BufferedReader getReader() throws IOException {
        return new BufferedReader(new InputStreamReader(getInputStream()));
    }

    public boolean isDirectory() throws IOException {
        Path path = new Path(this.path);
        Configuration conf = confFactory.createConfiguration();
        FileSystem dfs = FileSystem.get(conf);
        return dfs.isDirectory(path);
    }

    public HDFSSplit(DataInput input) throws IOException {
        int n = input.readInt();
        char[] cs = new char[n];
        for (int i = 0; i < n; i++)
            cs[i] = input.readChar();
        path = new String(cs);
        startOffset = input.readLong();
        length = input.readLong();
        n = input.readInt();
        hosts = new String[n];
        for (int i = 0; i < hosts.length; i++) {
            n = input.readInt();
            cs = new char[n];
            for (int j = 0; j < n; j++)
                cs[j] = input.readChar();
            hosts[i] = new String(cs);
        }
    }

    public void write(DataOutput output) throws IOException {
        output.writeInt(path.length());
        output.writeChars(path);
        output.writeLong(startOffset);
        output.writeLong(length);
        if (hosts == null) {
            output.writeInt(0);
        } else {
            output.writeInt(hosts.length);
            for (String s : hosts) {
                if (s == null)
                    output.writeInt(0);
                else {
                    output.writeInt(s.length());
                    output.writeChars(s);
                }
            }
        }
    }

    @Override
    public int hashCode() {
        return path.hashCode() + (int) startOffset + (int) length;
    }

    @Override
    public boolean equals(Object b) {
        if (!(b instanceof HDFSSplit))
            return false;
        HDFSSplit a = (HDFSSplit) b;
        if (!path.equals(a.path))
            return false;
        if (startOffset != a.startOffset)
            return false;
        if (length != a.length)
            return false;
        if ((params == null) != (a.params == null))
            return false;
        if (params != null) {
            if (!params.equals(a.params))
                return false;
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(path + "[" + startOffset + "," + length + "]");
        if (params != null) {
            sb.append("?");
            boolean first = true;
            for (String key : params.keySet()) {
                if (first)
                    first = false;
                else
                    sb.append("&");
                sb.append(key + "=" + params.get(key));
            }
        }
        return sb.toString();
    }
}
