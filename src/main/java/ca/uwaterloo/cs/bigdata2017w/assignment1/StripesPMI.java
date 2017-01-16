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
import tl.lin.data.map.HashMapWritable;
import tl.lin.data.pair.*;


import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Created by yanglinguan on 17/1/15.
 */
public class StripesPMI extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(StripesPMI.class);

    private static final class MyMapper extends Mapper<LongWritable, Text, Text, HashMapWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

        }
    }

    private static final class MyCombiner extends
            Reducer<Text, HashMapWritable, Text, HashMapWritable> {
        @Override
        public void reduce(Text key, Iterable<HashMapWritable> values, Context context)
                throws IOException, InterruptedException {

        }
    }

    private static final class MyReducer extends
            Reducer<Text, HashMapWritable, Text, HashMapWritable> {

    }

    /**
     * Creates an instance of this tool.
     */

    private StripesPMI() {}

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

        LOG.info("Tool: " + StripesPMI.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info(" - threshold: " + args.threshold);
        LOG.info(" - number of reducers: " + args.numReducers);

        // Delete the output directory if it exists already.
        Path outputDir = new Path(args.output);
        FileSystem.get(getConf()).delete(outputDir, true);

        Job job = Job.getInstance(getConf());
        job.setJobName(StripesPMI.class.getSimpleName());
        job.setJarByClass(StripesPMI.class);

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
        //job.setPartitionerClass(MyPartitioner.class);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new StripesPMI(), args);
    }


}
