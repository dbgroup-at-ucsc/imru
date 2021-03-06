package edu.uci.ics.hyracks.imru.elastic.wrapper;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import scala.actors.threadpool.Arrays;

import edu.uci.ics.hyracks.imru.api.ASyncIO;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api.ImruIterInfo;
import edu.uci.ics.hyracks.imru.api.ImruOptions;
import edu.uci.ics.hyracks.imru.api.ImruParameters;
import edu.uci.ics.hyracks.imru.api.ImruStream;
import edu.uci.ics.hyracks.imru.data.SerializedFrames;
import edu.uci.ics.hyracks.imru.elastic.ImruSendOperator;
import edu.uci.ics.hyracks.imru.elastic.test.DynamicAggregationStressTest;
import edu.uci.ics.hyracks.imru.elastic.test.DynamicMappingFunctionalTest;
import edu.uci.ics.hyracks.imru.file.ConfigurationFactory;
import edu.uci.ics.hyracks.imru.file.HDFSSplit;
import edu.uci.ics.hyracks.imru.file.IMRUInputSplitProvider;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUConnection;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRURuntimeContext;
import edu.uci.ics.hyracks.imru.util.Client;

public class IMRUMultiCore<Model extends Serializable, Data extends Serializable> {
    public static int networkSpeedInternal = 1024 * 1024; //B/s
    public static int frameSize = 32768;
    ImruOptions options;
    HDFSSplit[] splits;
    HDFSSplit[][] allocatedSplits;
    IMRURuntimeContext[] runtimeContexts;
    ImruSendOperator<Model, Data>[] os;
    IMRUConnection imruConnection = new IMRUConnection(null, -1) {
        Hashtable<String, byte[]> hash = new Hashtable<String, byte[]>();

        @Override
        public byte[] downloadData(String name) throws IOException {
            return hash.get(name);
        }

        @Override
        public void uploadData(String name, byte[] data) throws IOException {
            hash.put(name, data);
        }
    };
    ASyncIO<ByteBuffer>[] recvQueues;
    Thread[] recvThreads;
    ImruPlatformAPI writer;
    int[] targetPartitions;
    ImruParameters p;
    Model model;
    ImruStream<Model, Data> job;
    AtomicInteger runningOperators = new AtomicInteger(0);

    public IMRUMultiCore(ImruOptions options, Model model,
            ImruStream<Model, Data> job) throws Exception {
        this.options = options;
        this.model = model;
        this.job = job;
        if (options.disableLogging)
            Client.disableLogging();
        splits = IMRUInputSplitProvider.getInputSplits(options.inputPaths,
                new ConfigurationFactory(), options.numOfNodes
                        * options.splitsPerNode, 0, Long.MAX_VALUE);
        allocatedSplits = new HDFSSplit[options.numOfNodes][options.splitsPerNode];
        for (int i = 0; i < options.numOfNodes; i++) {
            for (int j = 0; j < options.splitsPerNode; j++) {
                allocatedSplits[i][j] = splits[i * options.splitsPerNode + j];
            }
        }
        targetPartitions = DynamicAggregationStressTest.getAggregationTree(
                options.numOfNodes, options.fanIn);
        p = new ImruParameters();
        p.dynamicAggr = options.dynamicAggr;
        p.dynamicMapping = options.dynamicMapping;
        p.dynamicMappersPerNode = options.dynamicMappersPerNode;
        p.useMemoryCache = options.memCache;
        p.dynamicDebug = options.dynamicDebug;
        p.disableSwapping = options.dynamicDisableSwapping;
        p.disableRelocation = options.dynamicDisableRelocation;
        if (!p.dynamicAggr || !p.dynamicMapping)
            throw new Error();
        writer = new ImruPlatformAPI() {
            @Override
            public void nextFrame(ByteBuffer buffer) throws IOException {
                int srcPartition = buffer
                        .getInt(SerializedFrames.SOURCE_OFFSET);
                int targetPartition = buffer
                        .getInt(SerializedFrames.TARGET_OFFSET);
                int writerId = buffer.getInt(SerializedFrames.WRITER_OFFSET);
                try {
                    byte[] bs = buffer.array();
                    bs = Arrays.copyOf(bs, bs.length);
                    if (bs.length != buffer.limit())
                        throw new Error();
                    recvQueues[writerId].add(ByteBuffer.wrap(bs));
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }

            @Override
            public IMRUContext getContext(String operatorName, int partition,
                    int nPartition) {
                return new IMRUContext("NC" + partition, frameSize,
                        runtimeContexts[partition], operatorName, partition,
                        nPartition);
            }

            @Override
            public ImruWriter createRunFileWriter() throws IOException {
                throw new IOException("Doesn't support disk cache yet");
            }

            @Override
            public void close() throws IOException {
                synchronized (runningOperators) {
                    runningOperators.decrementAndGet();
                    runningOperators.notifyAll();
                }
            }
        };
        recvQueues = new ASyncIO[options.numOfNodes];
        recvThreads = new Thread[options.numOfNodes];
        for (int i = 0; i < recvQueues.length; i++) {
            recvQueues[i] = new ASyncIO<ByteBuffer>(256);
            final int id = i;
            recvThreads[i] = new Thread("recv " + i) {
                @Override
                public void run() {
                    try {
                        Iterator<ByteBuffer> iterator = recvQueues[id]
                                .getInput();
                        while (iterator.hasNext()) {
                            ByteBuffer buffer = iterator.next();
                            int size = buffer.limit();
                            //                            Rt.p(size);
                            int ms = (int) ((float) size * 1000 / networkSpeedInternal);
                            Thread.sleep(ms);
                            os[id].recvFrame(buffer);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.exit(0);
                    }
                }
            };
            recvThreads[i].start();
        }
        runtimeContexts = new IMRURuntimeContext[options.numOfNodes];
        os = new ImruSendOperator[options.numOfNodes];
        while (true) {
            ImruIterInfo info = runIteration();
            if (job.shouldTerminate(model, info))
                break;
        }
        close();
    }

    ImruIterInfo runIteration() throws Exception {
        for (int i = 0; i < options.numOfNodes; i++)
            runtimeContexts[i] = new IMRURuntimeContext(null);
        for (int i = 0; i < options.numOfNodes; i++) {
            os[i] = new ImruSendOperator<Model, Data>(writer, i,
                    options.numOfNodes, targetPartitions, job, p,
                    options.modelFilename, imruConnection, splits,
                    allocatedSplits);
            os[i].runtimeContext.model = model;
            os[i].runtimeContext.modelAge = 0;
        }
        runningOperators.set(options.numOfNodes);
        for (int i = 0; i < options.numOfNodes; i++)
            os[i].startup();
        while (runningOperators.get() > 0) {
            synchronized (runningOperators) {
                runningOperators.wait();
            }
        }
        model = (Model) imruConnection.downloadModel(options.modelFilename);
        ImruIterInfo info = imruConnection
                .downloadDbgInfo(options.modelFilename);
        return info;
    }

    public void close() throws IOException {
        for (int i = 0; i < recvQueues.length; i++) {
            recvQueues[i].close();
        }
        //        IMRUSerialize.threadPool.shutdown();
    }

    public static <Model extends Serializable, Data extends Serializable> Model run(
            ImruOptions options, Model model, ImruStream<Model, Data> job)
            throws Exception {
        IMRUMultiCore<Model, Data> multiCore = new IMRUMultiCore<Model, Data>(
                options, model, job);
        return multiCore.model;
    }

    public static void main(String[] args) throws Exception {
        IMRUMultiCore.networkSpeedInternal = 1024 * 1024;

        ImruOptions options = new ImruOptions();
        options.inputPaths = "data/kmeans/kmeans0.txt";
        options.numOfNodes = 3;
        options.splitsPerNode = 2;
        options.memCache = true;
        options.modelFilename = "model";
        options.dynamicAggr = true;
        options.dynamicMapping = true;
        options.dynamicDisableRelocation = false;
        options.dynamicDisableSwapping = false;
        //        options.dynamicDebug = true;
        options.disableLogging = true;

        String model = "";
        ImruStream<String, String> job = new DynamicMappingFunctionalTest.Job();
        String finalModel = IMRUMultiCore.run(options, model, job);
        System.out.println("FinalModel: " + finalModel);
        System.exit(0);
    }
}
