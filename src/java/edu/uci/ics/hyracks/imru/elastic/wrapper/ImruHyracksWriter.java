package edu.uci.ics.hyracks.imru.elastic.wrapper;

import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.MapTaskState;
import edu.uci.ics.hyracks.imru.util.IterationUtils;

public class ImruHyracksWriter extends ImruPlatformAPI {
    DeploymentId deploymentId;
    IHyracksTaskContext ctx;
    public IFrameWriter writer;

    public ImruHyracksWriter(DeploymentId deploymentId,
            IHyracksTaskContext ctx, IFrameWriter writer) {
        this.deploymentId = deploymentId;
        this.ctx = ctx;
        this.writer = writer;
    }

    @Override
    public void open() throws HyracksDataException {
        writer.open();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        //        writer.nextFrame(buffer);
        FrameUtils.flushFrame(buffer, writer);
    }

    public void fail() throws HyracksDataException {
        writer.fail();
    }

    public void close() throws HyracksDataException {
        writer.close();
    }

    @Override
    public IMRUContext getContext(String operatorName, int partition,
            int nPartition) {
        return new IMRUContext(deploymentId, ctx, operatorName, partition,
                nPartition);
    }

    @Override
    public ImruWriter createRunFileWriter() throws IOException {
        FileReference file = ctx.createUnmanagedWorkspaceFile("IMRUInput");
        final RunFileWriter runFileWriter = new RunFileWriter(file, ctx
                .getIOManager());
        runFileWriter.open();
        return new ImruWriter() {
            @Override
            public void nextFrame(ByteBuffer buffer) throws IOException {
                runFileWriter.nextFrame(buffer);
            }

            @Override
            public void close() throws IOException {
                runFileWriter.close();
            }

            @Override
            public long getFileSize() {
                return runFileWriter.getFileSize();
            }

            @Override
            public String getPath() {
                return runFileWriter.getFileReference().getFile()
                        .getAbsolutePath();
            }

            @Override
            public ImruReader getReader() throws IOException {
                final RunFileReader reader = new RunFileReader(runFileWriter
                        .getFileReference(), ctx.getIOManager(), runFileWriter
                        .getFileSize());
                reader.open();
                return new ImruReader() {
                    @Override
                    public boolean nextFrame(ByteBuffer buffer)
                            throws IOException {
                        return reader.nextFrame(buffer);
                    }
                };
            }
        };
    }

    public void setIterationState(int partition, ImruState state) {
        MapTaskState state2 = new MapTaskState(state, ctx.getJobletContext()
                .getJobId(), ctx.getTaskAttemptId().getTaskId());
        IterationUtils.setIterationState(ctx, partition, state2);
    }

    public ImruState getIterationState(int partition) {
        MapTaskState s = (MapTaskState) IterationUtils.getIterationState(ctx,
                partition);
        if (s == null)
            return null;
        return s.state;
    }

    public void removeIterationState(int partition) {
        IterationUtils.removeIterationState(ctx, partition);
    }
}
