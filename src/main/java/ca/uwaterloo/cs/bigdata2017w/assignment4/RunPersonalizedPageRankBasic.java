package ca.uwaterloo.cs.bigdata2017w.assignment4;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.*;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import sun.security.util.AuthResources_de;
import tl.lin.data.array.ArrayListOfFloatsWritable;
import tl.lin.data.array.ArrayListOfIntsWritable;
import tl.lin.data.array.ArrayListOfLongs;
import tl.lin.data.map.HMapIF;
import tl.lin.data.map.HMapIV;
import tl.lin.data.map.MapIF;
import tl.lin.data.map.MapIV;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by yanglinguan on 17/2/2.
 */
public class RunPersonalizedPageRankBasic extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(RunPersonalizedPageRankBasic.class);
//    private static final String SOURCES_SIZE = "source node size";
    private static final ArrayListOfIntsWritable SOURCE_NODES = new ArrayListOfIntsWritable();

    private static final ArrayListOfFloatsWritable MISSING = new ArrayListOfFloatsWritable();

    private static enum PageRank {
        nodes, edges, massMessages, massMessagesSaved, massMessagesReceived, missingStructure
    };

    // Mapper with in-mapper combiner optimization.
    private static class MapWithInMapperCombiningClass extends
            Mapper<IntWritable, PersonalizedPageRankNode, IntWritable, PersonalizedPageRankNode> {
        // For buffering PageRank mass contributes keyed by destination node.
      //  private static final HMapIF map = new HMapIF();
        private static final HMapIV<ArrayListOfFloatsWritable> map = new HMapIV<>();
        // For passing along node structure.
        private static final PersonalizedPageRankNode intermediateStructure = new PersonalizedPageRankNode();

        @Override
        public void setup(Context context) throws IOException {
            // Note that this is needed for running in local mode due to mapper reuse.
            // We wouldn't need this is distributed mode.
            map.clear();
        }

        @Override
        public void map(IntWritable nid, PersonalizedPageRankNode node, Context context)
                throws IOException, InterruptedException {
            // Pass along node structure.
            intermediateStructure.setNodeId(node.getNodeId());
            intermediateStructure.setType(PageRankNode.Type.Structure);
            intermediateStructure.setAdjacencyList(node.getAdjacenyList());
            intermediateStructure.setSourceNode(SOURCE_NODES.size());

            context.write(nid, intermediateStructure);

            int massMessages = 0;
            int massMessagesSaved = 0;

            // Distribute PageRank mass to neighbors (along outgoing edges).
            if (node.getAdjacenyList().size() > 0) {
                // Each neighbor gets an equal share of PageRank mass.
                ArrayListOfIntsWritable list = node.getAdjacenyList();
                ArrayListOfFloatsWritable massList = node.getPageRankList();
                for(int i = 0; i < massList.size(); i++) {
                    massList.set(i, massList.get(i) - (float) StrictMath.log(list.size()));
                }

               // float mass = node.getPageRank() - (float) StrictMath.log(list.size());

                context.getCounter(PageRank.edges).increment(list.size());

                // Iterate over neighbors.
                for (int i = 0; i < list.size(); i++) {
                    int neighbor = list.get(i);

                    if (map.containsKey(neighbor)) {
                        // Already message destined for that node; add PageRank mass contribution.
                        massMessagesSaved++;
                        ArrayListOfFloatsWritable neighborMassList = map.get(neighbor);
                        ArrayListOfFloatsWritable update = new ArrayListOfFloatsWritable();
                        for(int t = 0; t < neighborMassList.size(); t++) {
                            update.add(sumLogProbs(neighborMassList.get(t), massList.get(t)));
                        }
                        map.put(neighbor, update);
                       // map.put(neighbor, sumLogProbs(map.get(neighbor), mass));
                    } else {
                        // New destination node; add new entry in map.
                        massMessages++;
                        map.put(neighbor, massList);
                    }
                }
            }

            // Bookkeeping.
            context.getCounter(PageRank.nodes).increment(1);
            context.getCounter(PageRank.massMessages).increment(massMessages);
            context.getCounter(PageRank.massMessagesSaved).increment(massMessagesSaved);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            // Now emit the messages all at once.
            IntWritable k = new IntWritable();
            PersonalizedPageRankNode mass = new PersonalizedPageRankNode();

            for (MapIV.Entry<ArrayListOfFloatsWritable> e : map.entrySet()) {
                k.set(e.getKey());

                mass.setNodeId(e.getKey());
                mass.setType(PageRankNode.Type.Mass);
                mass.setPageRankList(e.getValue());
                mass.setSourceNode(SOURCE_NODES.size());
                mass.setSource(SOURCE_NODES);

                context.write(k, mass);
            }
        }
    }

    // Combiner: sums partial PageRank contributions and passes node structure along.
    private static class CombineClass extends
            Reducer<IntWritable, PersonalizedPageRankNode, IntWritable, PersonalizedPageRankNode> {
        private static final PersonalizedPageRankNode intermediateMass = new PersonalizedPageRankNode();

        @Override
        public void reduce(IntWritable nid, Iterable<PersonalizedPageRankNode> values, Context context)
                throws IOException, InterruptedException {
            int massMessages = 0;

            // Remember, PageRank mass is stored as a log prob.
            ArrayListOfFloatsWritable mass = new ArrayListOfFloatsWritable(SOURCE_NODES.size());
            for(int i = 0; i < SOURCE_NODES.size(); i++) {
                mass.add(i, Float.NEGATIVE_INFINITY);
            }
                   // Float.NEGATIVE_INFINITY;
            for (PersonalizedPageRankNode n : values) {
                if (n.getType() == PageRankNode.Type.Structure) {
                    // Simply pass along node structure.
                    context.write(nid, n);
                } else {
                    // Accumulate PageRank mass contributions.
                    for(int i = 0; i < n.getPageRankList().size(); i++) {
                        mass.set(i, sumLogProbs(mass.get(i), n.getPageRankList().get(i)));
                    }
                   // mass = sumLogProbs(mass, n.getPageRank());
                    massMessages++;
                }
            }

            // Emit aggregated results.
            if (massMessages > 0) {
                intermediateMass.setNodeId(nid.get());
                intermediateMass.setType(PageRankNode.Type.Mass);
                intermediateMass.setPageRankList(mass);
                intermediateMass.setSourceNode(SOURCE_NODES.size());
                intermediateMass.setSource(SOURCE_NODES);

                context.write(nid, intermediateMass);
            }
        }
    }

    // Reduce: sums incoming PageRank contributions, rewrite graph structure.
    private static class ReduceClass extends
            Reducer<IntWritable, PersonalizedPageRankNode, IntWritable, PersonalizedPageRankNode> {
        // For keeping track of PageRank mass encountered, so we can compute missing PageRank mass lost
        // through dangling nodes.
       // private float totalMass = Float.NEGATIVE_INFINITY;
        private ArrayListOfFloatsWritable totalMass = new ArrayListOfFloatsWritable(SOURCE_NODES.size());

        @Override
        public void setup(Context context) {
            LOG.warn("source node size setup: " + SOURCE_NODES.size());
            for (int i = 0; i < SOURCE_NODES.size(); i++) {
                totalMass.add(i, Float.NEGATIVE_INFINITY);
            }
            LOG.warn("total mass size setup: " + totalMass.size());
        }

        @Override
        public void reduce(IntWritable nid, Iterable<PersonalizedPageRankNode> iterable, Context context)
                throws IOException, InterruptedException {
            Iterator<PersonalizedPageRankNode> values = iterable.iterator();

            // Create the node structure that we're going to assemble back together from shuffled pieces.
            PersonalizedPageRankNode node = new PersonalizedPageRankNode();

            node.setType(PageRankNode.Type.Complete);
            node.setNodeId(nid.get());
            node.setSourceNode(SOURCE_NODES.size());
            node.setSource(SOURCE_NODES);

            int massMessagesReceived = 0;
            int structureReceived = 0;

            //float mass = Float.NEGATIVE_INFINITY;
            ArrayListOfFloatsWritable mass = new ArrayListOfFloatsWritable(SOURCE_NODES.size());
            for(int i = 0; i < SOURCE_NODES.size(); i++) {
                mass.add(i, Float.NEGATIVE_INFINITY);
            }
            while (values.hasNext()) {
                PersonalizedPageRankNode n = values.next();

                if (n.getType().equals(PageRankNode.Type.Structure)) {
                    // This is the structure; update accordingly.
                    ArrayListOfIntsWritable list = n.getAdjacenyList();
                    structureReceived++;

                    node.setAdjacencyList(list);
                } else {
                    // This is a message that contains PageRank mass; accumulate.
                   // mass = sumLogProbs(mass, n.getPageRank());
                    for(int i = 0; i < n.getPageRankList().size(); i++) {
                        mass.set(i, sumLogProbs(mass.get(i), n.getPageRankList().get(i)));
                    }
                    massMessagesReceived++;
                }
            }

            // Update the final accumulated PageRank mass.
            node.setPageRankList(mass);
            context.getCounter(PageRank.massMessagesReceived).increment(massMessagesReceived);

            // Error checking.
            if (structureReceived == 1) {
                // Everything checks out, emit final node structure with updated PageRank value.
                context.write(nid, node);

                for(int i = 0 ; i < totalMass.size(); i++) {
                    totalMass.set(i, sumLogProbs(totalMass.get(i), mass.get(i)));
                }

                // Keep track of total PageRank mass.
               // totalMass = sumLogProbs(totalMass, mass);
            } else if (structureReceived == 0) {
                // We get into this situation if there exists an edge pointing to a node which has no
                // corresponding node structure (i.e., PageRank mass was passed to a non-existent node)...
                // log and count but move on.
                context.getCounter(PageRank.missingStructure).increment(1);
                LOG.warn("No structure received for nodeid: " + nid.get() + " mass: "
                        + massMessagesReceived);
                // It's important to note that we don't add the PageRank mass to total... if PageRank mass
                // was sent to a non-existent node, it should simply vanish.
            } else {
                // This shouldn't happen!
                throw new RuntimeException("Multiple structure received for nodeid: " + nid.get()
                        + " mass: " + massMessagesReceived + " struct: " + structureReceived);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String taskId = conf.get("mapred.task.id");
            String path = conf.get("PageRankMassPath");

            Preconditions.checkNotNull(taskId);
            Preconditions.checkNotNull(path);

            // Write to a file the amount of PageRank mass we've seen in this reducer.
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataOutputStream out = fs.create(new Path(path + "/" + taskId), false);
            totalMass.write(out);
            LOG.warn("Totoal mass size: " + totalMass.size());
            for(float f: totalMass) {
                LOG.warn("Total Mass: " + f );
            }

           //out.writeFloat(totalMass);
            out.close();
        }
    }

    // Mapper that distributes the missing PageRank mass (lost at the dangling nodes) and takes care
    // of the random jump factor.
    private static class MapPageRankMassDistributionClass extends
            Mapper<IntWritable, PersonalizedPageRankNode, IntWritable, PersonalizedPageRankNode> {
       //private float missingMass = 0.0f;
        private ArrayListOfFloatsWritable missingMass = new ArrayListOfFloatsWritable();
        private int nodeCnt = 0;

        @Override
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();

           // missingMass = conf.getFloat("MissingMass", 0.0f);
            nodeCnt = conf.getInt("NodeCount", 0);
        }

        @Override
        public void map(IntWritable nid, PersonalizedPageRankNode node, Context context)
                throws IOException, InterruptedException {

            if(SOURCE_NODES.contains(nid.get())) {
               // ArrayListOfFloatsWritable pList = node.getPageRankList();
                int idx = SOURCE_NODES.indexOf(nid.get());
                float p = node.getPageRankList().get(idx);
                p = sumLogProbs(p, (float) Math.log(MISSING.get(idx)));
                node.getPageRankList().set(idx, p);
                LOG.warn("Source: " + nid + ", pageRank: " + p);
            }

            context.write(nid, node);



            //float p = node.getPageRank();
//
//            float jump = (float) (Math.log(ALPHA) - Math.log(nodeCnt));
//            float link = (float) Math.log(1.0f - ALPHA)
//                    + sumLogProbs(p, (float) (Math.log(missingMass) - Math.log(nodeCnt)));
//
//            p = sumLogProbs(jump, link);
//            node.setPageRank(p);
//
//            context.write(nid, node);
        }
    }

    // Random jump factor.
   // private static float ALPHA = 0.15f;
    private static NumberFormat formatter = new DecimalFormat("0000");

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new RunPersonalizedPageRankBasic(), args);
    }

    public RunPersonalizedPageRankBasic() {}

    private static final String BASE = "base";
    private static final String NUM_NODES = "numNodes";
    private static final String START = "start";
    private static final String END = "end";
//    private static final String COMBINER = "useCombiner";
//    private static final String INMAPPER_COMBINER = "useInMapperCombiner";
//    private static final String RANGE = "range";
    private static final String SOURCES = "sources";

    /**
     * Runs this tool.
     */
    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {
        Options options = new Options();

//        options.addOption(new Option(COMBINER, "use combiner"));
//        options.addOption(new Option(INMAPPER_COMBINER, "user in-mapper combiner"));
//        options.addOption(new Option(RANGE, "use range partitioner"));

        //options.addOption(new Option())

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("base path").create(BASE));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("start iteration").create(START));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("end iteration").create(END));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of nodes").create(NUM_NODES));

        options.addOption(OptionBuilder.withArgName("sourceNodes").hasArg()
                .withDescription("source nodes").create(SOURCES));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(BASE) || !cmdline.hasOption(START) ||
                !cmdline.hasOption(END) || !cmdline.hasOption(NUM_NODES)
                || !cmdline.hasOption(SOURCES)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String basePath = cmdline.getOptionValue(BASE);
        int n = Integer.parseInt(cmdline.getOptionValue(NUM_NODES));
        int s = Integer.parseInt(cmdline.getOptionValue(START));
        int e = Integer.parseInt(cmdline.getOptionValue(END));
        String[] sourceNodes = cmdline.getOptionValue(SOURCES).split(",");

        for(String sn : sourceNodes) {
            SOURCE_NODES.add(Integer.parseInt(sn));
        }
//        boolean useCombiner = cmdline.hasOption(COMBINER);
//        boolean useInmapCombiner = cmdline.hasOption(INMAPPER_COMBINER);
//        boolean useRange = cmdline.hasOption(RANGE);

        LOG.info("Tool name: RunPersonalizedPageRankBasic");
        LOG.info(" - base path: " + basePath);
        LOG.info(" - num nodes: " + n);
        LOG.info(" - start iteration: " + s);
        LOG.info(" - end iteration: " + e);
        LOG.info(" - sources: " + cmdline.getOptionValue(SOURCES));
//        LOG.info(" - use combiner: " + useCombiner);
//        LOG.info(" - use in-mapper combiner: " + useInmapCombiner);
//        LOG.info(" - user range partitioner: " + useRange);

        // Iterate PageRank.
        for (int i = s; i < e; i++) {
            iteratePageRank(i, i + 1, basePath, n );
        }

        return 0;
    }

    // Run each iteration.
    private void iteratePageRank(int i, int j, String basePath, int numNodes) throws Exception {
        // Each iteration consists of two phases (two MapReduce jobs).

        // Job 1: distribute PageRank mass along outgoing edges.
        ArrayListOfFloatsWritable mass = phase1(i, j, basePath, numNodes);
        //float mass = phase1(i, j, basePath, numNodes);

        // Find out how much PageRank mass got lost at the dangling nodes.
        for(int t = 0; t < mass.size(); t++) {
            mass.set(t, 1.0f - (float) StrictMath.exp(mass.get(t)));
        }
       // float missing = 1.0f - (float) StrictMath.exp(mass);

        // Job 2: distribute missing mass, take care of random jump factor.
        //phase2(i, j, missing, basePath, numNodes);
        phase2(i, j, mass, basePath, numNodes);
    }

    private ArrayListOfFloatsWritable phase1(int i, int j, String basePath, int numNodes) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("PageRank:Basic:iteration" + j + ":Phase1");
        job.setJarByClass(RunPersonalizedPageRankBasic.class);

        String in = basePath + "/iter" + formatter.format(i);
        String out = basePath + "/iter" + formatter.format(j) + "t";
        String outm = out + "-mass";

        // We need to actually count the number of part files to get the number of partitions (because
        // the directory might contain _log).
        int numPartitions = 0;
        for (FileStatus s : FileSystem.get(getConf()).listStatus(new Path(in))) {
            if (s.getPath().getName().contains("part-"))
                numPartitions++;
        }

        LOG.info("PageRank: iteration " + j + ": Phase1");
        LOG.info(" - input: " + in);
        LOG.info(" - output: " + out);
        LOG.info(" - nodeCnt: " + numNodes);
//        LOG.info(" - useCombiner: " + useCombiner);
//        LOG.info(" - useInmapCombiner: " + useInMapperCombiner);
        LOG.info("computed number of partitions: " + numPartitions);

        int numReduceTasks = numPartitions;

        job.getConfiguration().setInt("NodeCount", numNodes);
        job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
        job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
        //job.getConfiguration().set("mapred.child.java.opts", "-Xmx2048m");
        job.getConfiguration().set("PageRankMassPath", outm);
//        job.getConfiguration().setInt(SOURCES_SIZE, sourceNodes.length);

        job.setNumReduceTasks(numReduceTasks);

        FileInputFormat.setInputPaths(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PersonalizedPageRankNode.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PersonalizedPageRankNode.class);
        job.setMapperClass(MapWithInMapperCombiningClass.class);

//        job.setMapperClass(useInMapperCombiner ? MapWithInMapperCombiningClass.class : MapClass.class);

//        if (useCombiner) {
      //      job.setCombinerClass(CombineClass.class);
//        }

        job.setReducerClass(ReduceClass.class);

        FileSystem.get(getConf()).delete(new Path(out), true);
        FileSystem.get(getConf()).delete(new Path(outm), true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        ArrayListOfFloatsWritable mass = new ArrayListOfFloatsWritable(SOURCE_NODES.size());
        for(int t = 0; t < SOURCE_NODES.size(); t++) {
            mass.add(t, Float.NEGATIVE_INFINITY);
        }
       // float mass = Float.NEGATIVE_INFINITY;
        FileSystem fs = FileSystem.get(getConf());
        for (FileStatus f : fs.listStatus(new Path(outm))) {
            FSDataInputStream fin = fs.open(f.getPath());
            ArrayListOfFloatsWritable fmass = new ArrayListOfFloatsWritable();
            fmass.readFields(fin);

            for(int t = 0; t < SOURCE_NODES.size(); t++) {
                mass.set(t, sumLogProbs(mass.get(t), fmass.get(t)));
                LOG.warn("miss mass " + SOURCE_NODES.get(t) + " " + t + " : " + sumLogProbs(mass.get(t), fmass.get(t)));
            }

           // mass = sumLogProbs(mass, fin.readFloat());
            fin.close();
        }

        return mass;
    }

    private void phase2(int i, int j, ArrayListOfFloatsWritable missing, String basePath, int numNodes) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("PageRank:Basic:iteration" + j + ":Phase2");
        job.setJarByClass(RunPersonalizedPageRankBasic.class);

        LOG.info("missing PageRank mass: " + missing);
        LOG.info("number of nodes: " + numNodes);

        String in = basePath + "/iter" + formatter.format(j) + "t";
        String out = basePath + "/iter" + formatter.format(j);

        LOG.info("PageRank: iteration " + j + ": Phase2");
        LOG.info(" - input: " + in);
        LOG.info(" - output: " + out);

        MISSING.clear();

        for(Float x: missing){
            MISSING.add(x);
        }

        job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
        job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
        //job.getConfiguration().set("MissingMass", (float) missing);
        job.getConfiguration().setInt("NodeCount", numNodes);

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PersonalizedPageRankNode.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PersonalizedPageRankNode.class);

        job.setMapperClass(MapPageRankMassDistributionClass.class);

        FileSystem.get(getConf()).delete(new Path(out), true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }

    // Adds two log probs.
    private static float sumLogProbs(float a, float b) {
        if (a == Float.NEGATIVE_INFINITY)
            return b;

        if (b == Float.NEGATIVE_INFINITY)
            return a;

        if (a < b) {
            return (float) (b + StrictMath.log1p(StrictMath.exp(a - b)));
        }

        return (float) (a + StrictMath.log1p(StrictMath.exp(b - a)));
    }
}
