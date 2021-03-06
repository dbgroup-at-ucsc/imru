package exp.imruVsSpark.kmeans.stratosphere;

import java.io.File;
import java.io.Serializable;
import java.util.Iterator;

import edu.uci.ics.hyracks.imru.util.CreateDeployment;
import eu.stratosphere.pact.client.PlanExecutor;
import eu.stratosphere.pact.client.RemoteExecutor;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.example.util.AsciiUtils;

/**
 * Implements a word count which takes the input file and counts the number of
 * the occurrences of each word in the file.
 */
public class WordCount implements PlanAssembler, PlanAssemblerDescription {

    /**
     * Converts a PactRecord containing one string in to multiple string/integer pairs.
     * The string is tokenized by whitespaces. For each token a new record is emitted,
     * where the token is the first field and an Integer(1) is the second field.
     */
    public static class TokenizeLine extends MapStub implements Serializable {
        private static final long serialVersionUID = 1L;

        // initialize reusable mutable objects
        private final PactRecord outputRecord = new PactRecord();
        private final PactString word = new PactString();
        private final PactInteger one = new PactInteger(1);

        private final AsciiUtils.WhitespaceTokenizer tokenizer = new AsciiUtils.WhitespaceTokenizer();

        @Override
        public void map(PactRecord record, Collector<PactRecord> collector) {
            // get the first field (as type PactString) from the record
            PactString line = record.getField(0, PactString.class);

            // normalize the line
            AsciiUtils.replaceNonWordChars(line, ' ');
            AsciiUtils.toLowerCase(line);

            // tokenize the line
            this.tokenizer.setStringToTokenize(line);
            while (tokenizer.next(this.word)) {
                // we emit a (word, 1) pair 
                this.outputRecord.setField(0, this.word);
                this.outputRecord.setField(1, this.one);
                collector.collect(this.outputRecord);
            }
        }
    }

    /**
     * Sums up the counts for a certain given key. The counts are assumed to be at position <code>1</code> in the record. The other fields are not modified.
     */
    @Combinable
    @ConstantFields(0)
    public static class CountWords extends ReduceStub implements Serializable {
        private static final long serialVersionUID = 1L;

        private final PactInteger cnt = new PactInteger();

        @Override
        public void reduce(Iterator<PactRecord> records,
                Collector<PactRecord> out) throws Exception {
            PactRecord element = null;
            int sum = 0;
            while (records.hasNext()) {
                element = records.next();
                PactInteger i = element.getField(1, PactInteger.class);
                sum += i.getValue();
            }

            this.cnt.setValue(sum);
            element.setField(1, this.cnt);
            out.collect(element);
        }

        /* (non-Javadoc)
         * @see eu.stratosphere.pact.common.stubs.ReduceStub#combine(java.util.Iterator, eu.stratosphere.pact.common.stubs.Collector)
         */
        @Override
        public void combine(Iterator<PactRecord> records,
                Collector<PactRecord> out) throws Exception {
            // the logic is the same as in the reduce function, so simply call the reduce method
            this.reduce(records, out);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Plan getPlan(String... args) {
        // parse job parameters
        int numSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
        String dataInput = (args.length > 1 ? args[1] : "");
        String output = (args.length > 2 ? args[2] : "");

        FileDataSource source = new FileDataSource(new TextInputFormat(),
                dataInput, "Input Lines");
        source.setParameter(TextInputFormat.CHARSET_NAME, "ASCII"); // comment out this line for UTF-8 inputs
        MapContract mapper = MapContract.builder(new TokenizeLine()).input(
                source).name("Tokenize Lines").build();
        ReduceContract reducer = ReduceContract.builder(CountWords.class,
                PactString.class, 0).input(mapper).name("Count Words").build();
        FileDataSink out = new FileDataSink(new RecordOutputFormat(), output,
                reducer, "Word Counts");
        RecordOutputFormat.configureRecordFormat(out).recordDelimiter('\n')
                .fieldDelimiter(' ').field(PactString.class, 0).field(
                        PactInteger.class, 1);

        Plan plan = new Plan(out, "WordCount Example");
        plan.setDefaultParallelism(numSubTasks);
        return plan;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDescription() {
        return "Parameters: [numSubStasks] [input] [output]";
    }

    public static void main(String[] args) throws Exception {
        WordCount wc = new WordCount();
        int nodes = 1;
        String input = "file://" + new File("run.sh").getAbsolutePath();
        String output = "file://"
                + new File("tmp/result.txt").getAbsolutePath();
        Plan plan = wc.getPlan("" + nodes, input, output);
        // This will create an executor to run the plan on a cluster. We assume
        // that the JobManager is running on the local machine on the default
        // port. Change this according to your configuration.
        File tmpJar = new File("tmp/stratosphere_wordcount.jar");
        CreateDeployment.createJar(new File("bin"), tmpJar);
        PlanExecutor ex = new RemoteExecutor("localhost", 6123, tmpJar
                .getAbsolutePath());
        ex.executePlan(plan);
    }
}