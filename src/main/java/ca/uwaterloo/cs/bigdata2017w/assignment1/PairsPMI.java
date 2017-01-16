package ca.uwaterloo.cs.bigdata2017w.assignment1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.*;


import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;


/**
 * Created by yanglinguan on 17/1/14.
 */
public class PairsPMI extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(PairsPMI.class);

    private static final class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, FloatWritable> {
        private static final FloatWritable ONE = new FloatWritable(1);
        private static final FloatWritable LINE = new FloatWritable();
        private static final PairOfStrings PAIR = new PairOfStrings();
        private static final Logger mapperLog = Logger.getLogger(MyMapper.class);
        private static final int MAX = 40;
        private int lineno = 0;

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            List<String> tokens = Tokenizer.tokenize(value.toString());
            lineno++;
            int size = Math.min(MAX, tokens.size());
            // unique pair of strings
            HashMap<PairOfStrings, Integer> countsPair = new HashMap();
            // unique words;
            HashMap<String, Integer> countsWords = new HashMap();

            for(int i = 0; i < size; i++) {
                for(int j = 0; j < size; j++) {
                    if(i == j) continue;
                    String w1 = tokens.get(i);
                    String w2 = tokens.get(j);
                    if(w1.equals(w2)) continue;
                    PAIR.set(tokens.get(i), tokens.get(j));
                    countsPair.put(PAIR, 1);
                }
                //PAIR.set(tokens.get(i), "*");
                countsWords.put(tokens.get(i), 1);
               // context.write(PAIR, ONE);
            }

            for(PairOfStrings k : countsPair.keySet()) {
                context.write(k, ONE);
            }

            for(String k: countsWords.keySet()) {
                PAIR.set("*", k);
                context.write(PAIR, ONE);
            }

        }
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            PAIR.set("*", "*");
            LINE.set(lineno);
            context.write(PAIR, LINE);
        }
    }

    private static final class MyCombiner extends
            Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {
        private static final FloatWritable SUM = new FloatWritable();

        @Override
        public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            Iterator<FloatWritable> iter = values.iterator();
            while (iter.hasNext()) {
                sum += iter.next().get();
            }

            SUM.set(sum);
            context.write(key, SUM);
        }
    }

    private static final class MyReducer extends
            Reducer<PairOfStrings, FloatWritable, PairOfStrings, PairOfFloatInt> {
        private static final PairOfFloatInt VALUE = new PairOfFloatInt();

        private float line = 0.0f;

        private int threshold = 1;

        private HashMap<String, Float> counts = new HashMap<String, Float>();


        @Override
        public void setup(Reducer.Context context) {
            threshold = context.getConfiguration().getInt("threshold", 1);
        }

        @Override
        public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            float sum = 0.0f;
            Iterator<FloatWritable> iter = values.iterator();
            while (iter.hasNext()) {
                sum += iter.next().get();
            }


            if(sum >= threshold) {
                if (key.getLeftElement().equals("*")) {
                    if (key.getRightElement().equals("*")) {
                        //VALUE.set(1, (int) sum);
                        //context.write(key, VALUE);
                        line = sum;
                    } else {
                        counts.put(key.getRightElement(), sum);
                       // VALUE.set(1, (int) sum);
                        //context.write(key, VALUE);
                        //line = sum;
                    }
                } else {
                    //if (sum >= threshold) {
                        float ne = sum * line;
                        float de = counts.get(key.getRightElement()) * counts.get(key.getLeftElement());
                        VALUE.set((float) (Math.log10(ne / de)), (int) sum);
                        context.write(key, VALUE);
                    //}
                }
            }

        }
    }

    private static final class MyPartitioner extends Partitioner<PairOfStrings, FloatWritable> {
        @Override
        public int getPartition(PairOfStrings key, FloatWritable value, int numReduceTasks) {
            return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    /**
     * Creates an instance of this tool.
     */

    private PairsPMI() {}

    private static final class Args {
        @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
        String input;

        @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
        String output;

        @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
        int numReducers = 1;

        @Option(name = "-threshold", metaVar = "[num]", usage = "threshold of co-occurrence")
        int threshold = 1;
    }

    /**
     * Runs this tool.
     */
    @Override
    public int run(String[] argv) throws Exception {
        final Args args = new Args();
        CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

        try {
            parser.parseArgument(argv);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            return -1;
        }

        LOG.info("Tool: " + PairsPMI.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info(" - threshold: " + args.threshold);
        LOG.info(" - number of reducers: " + args.numReducers);

        // Delete the output directory if it exists already.
        Path outputDir = new Path(args.output);
        FileSystem.get(getConf()).delete(outputDir, true);

        Job job = Job.getInstance(getConf());
        job.setJobName(PairsPMI.class.getSimpleName());
        job.setJarByClass(PairsPMI.class);

        job.getConfiguration().setInt("threshold", args.threshold);

        job.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(job, new Path(args.input));
        FileOutputFormat.setOutputPath(job, new Path(args.output));

        job.setMapOutputKeyClass(PairOfStrings.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setOutputKeyClass(PairOfStrings.class);
        job.setOutputValueClass(PairOfFloatInt.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyCombiner.class);
        job.setReducerClass(MyReducer.class);
        job.setPartitionerClass(MyPartitioner.class);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new PairsPMI(), args);
    }

}
