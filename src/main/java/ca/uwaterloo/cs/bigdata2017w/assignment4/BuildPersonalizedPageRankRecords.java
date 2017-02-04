package ca.uwaterloo.cs.bigdata2017w.assignment4;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import tl.lin.data.array.ArrayListOfIntsWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by yanglinguan on 17/2/2.
 */
public class BuildPersonalizedPageRankRecords extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(BuildPersonalizedPageRankRecords.class);

    private static final String NODE_CNT_FIELD = "node.cnt";

    private static final String SOURCES_SIZE = "source node size";

    private static final ArrayListOfIntsWritable SOURCE_NODES = new ArrayListOfIntsWritable();

    private static class MyMapper extends Mapper<LongWritable, Text, IntWritable, PersonalizedPageRankNode> {
        private static final IntWritable nid = new IntWritable();
        private static final PersonalizedPageRankNode node = new PersonalizedPageRankNode();

        @Override
        public void setup(Mapper<LongWritable, Text, IntWritable, PersonalizedPageRankNode>.Context context) {
            int n = context.getConfiguration().getInt(NODE_CNT_FIELD, 0);
            if (n == 0) {
                throw new RuntimeException(NODE_CNT_FIELD + " cannot be 0!");
            }
            int sn = context.getConfiguration().getInt(SOURCES_SIZE, 0);
            if(sn == 0) {
                throw new RuntimeException(SOURCES_SIZE + " cannot be 0!");
            }
            node.setSourceNode(sn);
            node.setType(PageRankNode.Type.Complete);
            node.setSource(SOURCE_NODES);
//            for(int i: SOURCE_NODES) {
//                node.setPageRank(i, (float) StrictMath.log(1));
//            }
           // node.setPageRank((float) -StrictMath.log(n));
        }

        @Override
        public void map(LongWritable key, Text t, Context context) throws IOException,
                InterruptedException {
            String[] arr = t.toString().trim().split("\\s+");
            int id = Integer.parseInt(arr[0]);
            nid.set(id);
            for(int i: SOURCE_NODES) {
                int idx = SOURCE_NODES.indexOf(i);
                if(i == id) {
                   // node.setPageRank(idx, 1f);
                    node.setPageRank(idx, (float) StrictMath.log(1));
                } else {
                    //node.setPageRank(idx, 0f);
                    node.setPageRank(idx, (float) StrictMath.log(0));
                }
            }

            if (arr.length == 1) {
                node.setNodeId(id);
                node.setAdjacencyList(new ArrayListOfIntsWritable());

            } else {
                node.setNodeId(id);

                int[] neighbors = new int[arr.length - 1];
                for (int i = 1; i < arr.length; i++) {
                    neighbors[i - 1] = Integer.parseInt(arr[i]);
                }

                node.setAdjacencyList(new ArrayListOfIntsWritable(neighbors));
            }

            context.getCounter("graph", "numNodes").increment(1);
            context.getCounter("graph", "numEdges").increment(arr.length - 1);

            if (arr.length > 1) {
                context.getCounter("graph", "numActiveNodes").increment(1);
            }

            context.write(nid, node);
        }
    }

    public BuildPersonalizedPageRankRecords() {}

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String NUM_NODES = "numNodes";
    private static final String SOURCE = "sources";

    /**
     * Runs this tool.
     */
    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of nodes").create(NUM_NODES));
        options.addOption(OptionBuilder.withArgName("sources").hasArg()
                .withDescription("source nodes").create(SOURCE));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(NUM_NODES)
                || !cmdline.hasOption(SOURCE)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        int n = Integer.parseInt(cmdline.getOptionValue(NUM_NODES));
        String[] sourceNodes = cmdline.getOptionValue(SOURCE).split(",");

        for(String s : sourceNodes) {
            SOURCE_NODES.add(Integer.parseInt(s));
        }

        LOG.info("Tool name: " + BuildPersonalizedPageRankRecords.class.getSimpleName());
        LOG.info(" - inputDir: " + inputPath);
        LOG.info(" - outputDir: " + outputPath);
        LOG.info(" - numNodes: " + n);
        LOG.info(" - sources: " + cmdline.getOptionValue(SOURCE));

        Configuration conf = getConf();
        conf.setInt(NODE_CNT_FIELD, n);
        conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
        conf.setInt(SOURCES_SIZE, sourceNodes.length);

        Job job = Job.getInstance(conf);
        job.setJobName(BuildPersonalizedPageRankRecords.class.getSimpleName() + ":" + inputPath);
        job.setJarByClass(BuildPersonalizedPageRankRecords.class);

        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PersonalizedPageRankNode.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PersonalizedPageRankNode.class);

        job.setMapperClass(MyMapper.class);

        // Delete the output directory if it exists already.
        FileSystem.get(conf).delete(new Path(outputPath), true);

        job.waitForCompletion(true);

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new BuildPersonalizedPageRankRecords(), args);
    }
}
