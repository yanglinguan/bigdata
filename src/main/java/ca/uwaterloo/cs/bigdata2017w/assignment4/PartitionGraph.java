package ca.uwaterloo.cs.bigdata2017w.assignment4;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.util.Arrays;

/**
 * Created by yanglinguan on 17/2/2.
 */
public class PartitionGraph extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(PartitionGraph.class);

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new PartitionGraph(), args);
    }

    public PartitionGraph() {}

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String NUM_NODES = "numNodes";
    private static final String NUM_PARTITIONS = "numPartitions";
    private static final String RANGE = "range";

    /**
     * Runs this tool.
     */
    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(new Option(RANGE, "use range partitioner"));

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of nodes").create(NUM_NODES));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of partitions").create(NUM_PARTITIONS));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(NUM_NODES)
                || !cmdline.hasOption(NUM_PARTITIONS)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String inPath = cmdline.getOptionValue(INPUT);
        String outPath = cmdline.getOptionValue(OUTPUT);
        int nodeCount = Integer.parseInt(cmdline.getOptionValue(NUM_NODES));
        int numParts = Integer.parseInt(cmdline.getOptionValue(NUM_PARTITIONS));
        boolean useRange = cmdline.hasOption(RANGE);

        LOG.info("Tool name: " + PartitionGraph.class.getSimpleName());
        LOG.info(" - input dir: " + inPath);
        LOG.info(" - output dir: " + outPath);
        LOG.info(" - num partitions: " + numParts);
        LOG.info(" - node cnt: " + nodeCount);
        LOG.info(" - use range partitioner: " + useRange);

        Configuration conf = getConf();
        conf.setInt("NodeCount", nodeCount);

        Job job = Job.getInstance(conf);
        job.setJobName(PartitionGraph.class.getSimpleName() + ":" + inPath);
        job.setJarByClass(PartitionGraph.class);

        job.setNumReduceTasks(numParts);

        FileInputFormat.setInputPaths(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PersonalizedPageRankNode.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PersonalizedPageRankNode.class);

        if (useRange) {
            job.setPartitionerClass(RangePartitioner.class);
        }

        FileSystem.get(conf).delete(new Path(outPath), true);

        job.waitForCompletion(true);

        return 0;
    }
}
