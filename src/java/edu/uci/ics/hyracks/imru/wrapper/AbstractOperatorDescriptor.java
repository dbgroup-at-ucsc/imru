//package edu.uci.ics.hyracks.imru.wrapper;
//
//import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
//import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
//import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
//import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
//
//public class AbstractOperatorDescriptor extends
//        AbstractSingleActivityOperatorDescriptor {
//    private static final RecordDescriptor dummyRecordDescriptor = new RecordDescriptor(
//            new ISerializerDeserializer[1]);
//
//    public AbstractOperatorDescriptor(AbstractJobSpecification spec,
//            int inputArity, int outputArity) {
//        super(null, inputArity, outputArity);
//        recordDescriptors[0] = dummyRecordDescriptor;
//    }
//
////    public void setDisplayName(String displayName) {
////        setDisplayName(displayName);
////    }
//}
