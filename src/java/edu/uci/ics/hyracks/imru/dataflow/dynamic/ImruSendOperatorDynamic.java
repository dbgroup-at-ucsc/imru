package edu.uci.ics.hyracks.imru.dataflow.dynamic;

import java.io.Serializable;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class ImruSendOperatorDynamic<Model extends Serializable, Data extends Serializable>
        extends AbstractUnaryOutputSourceOperatorNodePushable {
    ImruSendOperator<Model, Data> so;

    public ImruSendOperatorDynamic(ImruSendOperator<Model, Data> so) {
        this.so = so;
    }

    @Override
    public void initialize() throws HyracksDataException {
        so.initialize();
    }
}
