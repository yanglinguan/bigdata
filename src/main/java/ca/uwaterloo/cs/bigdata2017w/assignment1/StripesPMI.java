package ca.uwaterloo.cs.bigdata2017w.assignment1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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
import tl.lin.data.map.HMapStFW;
import tl.lin.data.map.HashMapWritable;
import tl.lin.data.pair.*;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by yanglinguan on 17/1/15.
 */
public class StripesPMI extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(StripesPMI.class);

    // Job1 Mapper
    // counter number of lines that x appears, y appears, and number of total lines
    private static final class Job1Mapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private static final FloatWritable ONE = new FloatWritable(1.0f);
        private static final FloatWritable LINE = new FloatWritable();
        private static final Text KEY = new Text();
        private static final int MAX = 40;
        private float lineno = 0.0f;

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            List<String> tokens = Tokenizer.tokenize(value.toString());
            lineno += 1.0f;
            int size = Math.min(MAX, tokens.size());

            // unique words;
            HashSet<String> countsWords = new HashSet<>();

            for(int i = 0; i < size; i++) {
                countsWords.add(tokens.get(i));
            }

            for(String k: countsWords) {
                KEY.set(k);
                context.write(KEY, ONE);
            }

        }
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            KEY.set("*");
            LINE.set(lineno);
            context.write(KEY, LINE);
        }
    }

    // Job1 Combiner
    private static final class Job1Combiner extends
            Reducer<Text, FloatWritable, Text, FloatWritable> {
        private static final FloatWritable SUM = new FloatWritable();

        @Override
        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            float sum = 0.0f;
            Iterator<FloatWritable> iter = values.iterator();
            while (iter.hasNext()) {
                sum += iter.next().get();
            }

            SUM.set(sum);
            context.write(key, SUM);
        }
    }

    // Job1 Reducer
    private static final class Job1Reducer extends
            Reducer<Text, FloatWritable, Text, FloatWritable> {
        private static final FloatWritable VALUE = new FloatWritable();

        private int threshold = 1;

        @Override
        public void setup(Context context) throws IOException {
            threshold = context.getConfiguration().getInt("threshold", 1);
        }

        @Override
        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            float sum = 0.0f;
            Iterator<FloatWritable> iter = values.iterator();
            while (iter.hasNext()) {
                sum += iter.next().get();
            }

            if(sum >= threshold) {
                VALUE.set(sum);
                context.write(key, VALUE);
            }
        }
    }

    // Job2 count number of lines that contains unique (x, y) pair
    // Job2 Mapper
    private static final class Job2Mapper extends Mapper<LongWritable, Text, Text, HMapStFW> {

        private static final Text KEY = new Text();
        private static final int MAX = 40;

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            List<String> tokens = Tokenizer.tokenize(value.toString());

            int size = Math.min(MAX, tokens.size());

            HashSet<String> uniqueWords = new HashSet<>();

            for(int i = 0; i < size; i++) {
                uniqueWords.add(tokens.get(i));
            }

            for(String w1: uniqueWords) {
                HMapStFW strip = new HMapStFW();
                for(String w2: uniqueWords) {
                    if(!w1.equals(w2)) {
                        strip.put(w2, 1.0f);
                    }
                }

                KEY.set(w1);
                context.write(KEY, strip);
            }
        }
    }


    private static final class Job2Combiner extends Reducer<Text, HMapStFW, Text, HMapStFW> {
        @Override
        public void reduce(Text key, Iterable<HMapStFW> values, Context context)
                throws IOException, InterruptedException {
            Iterator<HMapStFW> iter = values.iterator();
            HMapStFW map = new HMapStFW();

            while (iter.hasNext()) {
                map.plus(iter.next());
            }

            context.write(key, map);
        }
    }

    // Job2 Reducer
    private static final class Job2Reducer extends
            Reducer<Text, HMapStFW, Text, HashMapWritable<Text, PairOfFloats>> {

        private int threshold = 1;

        private float lineno = 0.0f;

        private HashMap<String, Float> counter = new HashMap<>();



        @Override
        public void setup(Context context) throws IOException {
            //System.out.println("**********************************");
            threshold = context.getConfiguration().getInt("threshold", 1);
            // LOG.info("threshold: " + threshold);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path f = new Path("counterJobStripe/part-r-00000");

            if (!fs.exists(f)) {
                throw new IOException("File does not found " + f.toString() + "Job1 error");
            }

            BufferedReader br;

            FSDataInputStream input = fs.open(f);
            InputStreamReader stream = new InputStreamReader(input, "UTF-8");


            br = new BufferedReader(stream);

            String key = "";
            float value;

            String line = br.readLine();
            while (line != null) {
                //LOG.info("Line: " + line);
                StringTokenizer itr = new StringTokenizer(line);
                List<String> tokens = Tokenizer.tokenize(line);

                if(tokens.size() > 1) {
                    throw new IOException("job1 error: counter too many tokens more than 1");
                }

                if(itr.hasMoreTokens()) {
                    if(tokens.size() == 1) {
                        key = tokens.get(0);
                    } else {
                        key = "*";
                    }
                    itr.nextToken();
                }
                if(itr.hasMoreTokens()) {
                    value = Float.parseFloat(itr.nextToken());
                } else {
                    throw new IOException("Too less tokens");
                }

                if(itr.hasMoreTokens()) {
                    throw new IOException("Too many tokens");
                }

                if(key.equals("*")) {
                    lineno = value;
                } else {
                    counter.put(key, value);
                }
                line = br.readLine();
            }
            br.close();
        }

        @Override
        public void reduce(Text key, Iterable<HMapStFW> values, Context context)
                throws IOException, InterruptedException {
            Iterator<HMapStFW> iter = values.iterator();
            HMapStFW map = new HMapStFW();

            while (iter.hasNext()) {
                map.plus(iter.next());
            }

            HashMapWritable<Text, PairOfFloats> VALUE = new HashMapWritable<>();

            for (String term : map.keySet()) {
                if(map.get(term) >= threshold) {
                    float ne = map.get(term) * lineno;
                    if(!counter.containsKey(key.toString()) || !counter.containsKey(term)) {
                        if (!counter.containsKey(key.toString())) {
                            throw new IOException("Cannot found: key: " + key.toString());
                        } else {
                            throw new IOException("Cannot found: term: " + term);
                        }
                    } else {
                        float de = counter.get(key.toString()) * counter.get(term);
                        float pmi = (float) Math.log10(ne / de);
                        PairOfFloats PAIR = new PairOfFloats();
                        Text KEY = new Text();
                        PAIR.set(pmi, map.get(term));
                        KEY.set(term);
                        if (term.equals(key.toString())) {
                            LOG.info("term: key : " + term);
                        }
                        VALUE.put(KEY, PAIR);
                    }
                }
            }

            if( VALUE.size() > 0) {
                context.write(key, VALUE);
            }
        }
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

        String interPath = "counterJobStripe";

        LOG.info("Tool: " + StripesPMI.class.getSimpleName() + "Job1:Counter");
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + interPath);
        LOG.info(" - threshold: " + args.threshold);
        LOG.info(" - number of reducers: " + 1);

        long totalTimeStart = System.currentTimeMillis();

        Job job1 = Job.getInstance(getConf());
        job1.setJobName(StripesPMI.class.getSimpleName() + "Counter");
        job1.setJarByClass(StripesPMI.class);


        job1.getConfiguration().setStrings("job1Path", interPath);
        job1.getConfiguration().setInt("threshold", args.threshold);

        job1.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job1, new Path(args.input));
        FileOutputFormat.setOutputPath(job1, new Path(interPath));

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(FloatWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(FloatWritable.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        job1.setMapperClass(Job1Mapper.class);
        job1.setCombinerClass(Job1Combiner.class);
        job1.setReducerClass(Job1Reducer.class);

        job1.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
        job1.getConfiguration().set("mapreduce.map.memory.mb", "3072");
        job1.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
        job1.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
        job1.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");


        // Delete the output directory if it exists already.
        Path job1Path = new Path(interPath);
        FileSystem.get(getConf()).delete(job1Path, true);

        long startTime1 = System.currentTimeMillis();
        job1.waitForCompletion(true);
        System.out.println("Stripes Job1 Finished in " + (System.currentTimeMillis() - startTime1) / 1000.0 + " seconds");


        // job2
        LOG.info("Tool: " + StripesPMI.class.getSimpleName() + "Job2:PMI");
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info(" - threshold: " + args.threshold);
        LOG.info(" - number of reducers: " + args.numReducers);

        Job job2 = Job.getInstance(getConf());
        job2.setJobName(StripesPMI.class.getSimpleName() + "Job2:PMI");
        job2.setJarByClass(StripesPMI.class);

        job2.getConfiguration().setStrings("job1Path", interPath);
        job2.getConfiguration().setInt("threshold", args.threshold);

        job2.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(job2, new Path(args.input));
        FileOutputFormat.setOutputPath(job2, new Path(args.output));

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(HMapStFW.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(HashMapWritable.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        job2.setMapperClass(Job2Mapper.class);
        job2.setCombinerClass(Job2Combiner.class);
        job2.setReducerClass(Job2Reducer.class);

        // Delete the output directory if it exists already.
        Path outputDir = new Path(args.output);
        FileSystem.get(getConf()).delete(outputDir, true);

        job2.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
        job2.getConfiguration().set("mapreduce.map.memory.mb", "3072");
        job2.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
        job2.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
        job2.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");


        long startTime2 = System.currentTimeMillis();
        job2.waitForCompletion(true);
        System.out.println("Stripes Job2 Finished in " + (System.currentTimeMillis() - startTime2) / 1000.0 + " seconds");
        System.out.println("Total Finished in " + (System.currentTimeMillis() - totalTimeStart) / 1000.0 + " seconds");
        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new StripesPMI(), args);
    }
}
