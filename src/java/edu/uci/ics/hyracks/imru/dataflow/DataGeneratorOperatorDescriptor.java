package edu.uci.ics.hyracks.imru.dataflow;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorNodePushable;
import edu.uci.ics.hyracks.imru.api.IIMRUDataGenerator;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.file.ConfigurationFactory;
import edu.uci.ics.hyracks.imru.file.HDFSSplit;
import edu.uci.ics.hyracks.imru.util.Rt;

public class DataGeneratorOperatorDescriptor extends
        IMRUOperatorDescriptor<Serializable, Serializable> {
    private static final Logger LOG = Logger
            .getLogger(DataGeneratorOperatorDescriptor.class.getName());

    private static final long serialVersionUID = 1L;

    protected final ConfigurationFactory confFactory;
    protected final HDFSSplit[] inputSplits;
    IIMRUDataGenerator imruSpec;
    DeploymentId deploymentId;

    public DataGeneratorOperatorDescriptor(DeploymentId deploymentId,
            JobSpecification spec, IIMRUDataGenerator imruSpec,
            HDFSSplit[] inputSplits, ConfigurationFactory confFactory) {
        super(spec, 0, 0, "parse", null);
        this.deploymentId = deploymentId;
        this.inputSplits = inputSplits;
        this.confFactory = confFactory;
        this.imruSpec = imruSpec;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(
            final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition,
            final int nPartitions) throws HyracksDataException {
        return new AbstractOperatorNodePushable() {
            private final String name;
            long startTime;
            IMRUContext imruContext;
            boolean initialized = false;

            {
                name = DataGeneratorOperatorDescriptor.this.getDisplayName()
                        + partition;
            }

            @Override
            public void initialize() throws HyracksDataException {
                if (initialized)
                    return;
                initialized = true;
                startTime = System.currentTimeMillis();

                imruContext = new IMRUContext(deploymentId, ctx, name,
                        partition, nPartitions);
                final HDFSSplit split = inputSplits[partition];
                try {
                    BufferedOutputStream output = new BufferedOutputStream(
                            new FileOutputStream(split.getPath()), 1024 * 1024);
                    imruSpec.generate(imruContext, output);
                    output.close();
                } catch (IOException e) {
                    fail();
                    Rt.p(imruContext.getNodeId() + " " + split);
                    throw new HyracksDataException(e);
                }
                LOG.info("Generate input data in "
                        + (System.currentTimeMillis() - startTime)
                        + " milliseconds");
            }

            @Override
            public void setOutputFrameWriter(int index, IFrameWriter writer,
                    RecordDescriptor recordDesc) {
                throw new IllegalArgumentException();
            }

            @Override
            public void deinitialize() throws HyracksDataException {
            }

            @Override
            public int getInputArity() {
                return 0;
            }

            @Override
            public final IFrameWriter getInputFrameWriter(int index) {
                throw new IllegalArgumentException();
            }

            private void fail() throws HyracksDataException {
            }
        };
    }
}