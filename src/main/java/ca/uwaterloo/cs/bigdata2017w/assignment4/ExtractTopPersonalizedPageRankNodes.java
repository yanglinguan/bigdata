package ca.uwaterloo.cs.bigdata2017w.assignment4;

import org.apache.commons.cli.*;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.apache.log4j.Logger;
import scala.tools.cmd.gen.AnyVals;
import tl.lin.data.array.ArrayListOfFloatsWritable;
import tl.lin.data.array.FloatArrayWritable;

import java.io.EOFException;
import java.io.IOException;
import java.util.*;

/**
 * Created by yanglinguan on 17/2/2.
 */
public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);

    private ExtractTopPersonalizedPageRankNodes() {}

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(), args);
    }

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String TOP = "top";
    private static final String SOURCE = "sources";

    private static final ArrayList<Integer> SOURCE_NODES = new ArrayList<>();

    private static int topValue = 0;

   // private static ArrayList<SequenceFile.Reader> iter = new ArrayList<>();
   // private static final ArrayList<ArrayList<Pair<Float, Integer>>> topList = new ArrayList<>();

    private static HashMap<Integer, ArrayList<Pair<Float, Integer>>> topList = new HashMap<>();

    private void addTopList(int s, float p, int id) {
        //int idx = SOURCE_NODES.indexOf(s);
     //   LOG.info("key: " + id + ", page rank: " + p);
        if(topList.containsKey(s)) {
            int size = Math.min(topValue, topList.get(s).size());
            if(size == 0) {
                topList.get(s).add(new Pair<>(p, id));
            } else {
                boolean added = false;
                for (int i = 0; i < size; i++) {
                    if (p > topList.get(s).get(i).getFirst()) {
                        topList.get(s).add(i, new Pair<>(p, id));
                    //    LOG.info("added3 key: " + id + ", page rank: " + p);
                        break;
                    }
                }
                if(size < topValue && !added) {
                    topList.get(s).add(new Pair<>(p, id));
                }
            }
        } else {
            ArrayList<Pair<Float, Integer>> v = new ArrayList<>();
            v.add(new Pair<>(p, id));
            LOG.info("added4 key: " + id + ", page rank: " + p);
            topList.put(s, v);
        }

    }

    private void getTop(String iterPath, FileSystem fs) throws IOException {
        for (FileStatus f : fs.listStatus(new Path(iterPath))) {
            if (f.getPath().getName().startsWith("part-")) {
                LOG.info("file: " + f.getPath().getName());
                SequenceFile.Reader reader = new SequenceFile.Reader(fs, f.getPath(), fs.getConf());
                //FSDataInputStream fin = fs.open(f.getPath());

                while (true) {
                    try {
                        PageRankNode node = new PageRankNode();
                        IntWritable key = new IntWritable();
                        //int key = fin.readInt();

                        reader.next(key, node);


                       // LOG.info("type: " + node.getType());
                        if(node.getType() != PageRankNode.Type.Complete) {
                            break;
                        }
                       // LOG.info("key: " + key + ", page rank: " + node.getPageRank() + " ");
//                                + node.getPageRankList().get(1) + node.getPageRankList().get(2));

                        for (int s : SOURCE_NODES) {
                            if(node.getType() == PageRankNode.Type.Complete) {
                                addTopList(s, (float) StrictMath.exp(node.getPageRank()), node.getNodeId());
                            }
                        }

                    } catch (EOFException e) {
                        reader.close();
                        break;
                    }
                }
            }
        }
    }

    private void writeResult(FileSystem fs, String outPath) {
        for(int s: SOURCE_NODES) {
            System.out.println("Source: " + s);
            for(int i = 0; i < topValue; i++) {
                String printString = String.format("%.5f %d", topList.get(s).get(i).getFirst(), topList.get(s).get(i).getSecond());
                System.out.println(printString);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("").create(TOP));
        options.addOption(OptionBuilder.withArgName("list").hasArg()
                .withDescription("source nodes").create(SOURCE));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(TOP)
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
        int top = Integer.parseInt(cmdline.getOptionValue(TOP));
        String[] sourceNodes = cmdline.getOptionValue(SOURCE).split(",");
        topValue = top;

        for(String s : sourceNodes) {

//            topList.put(Integer.parseInt(s), new ArrayList<Float>(top));

            SOURCE_NODES.add(Integer.parseInt(s));
            topList.put(Integer.parseInt(s), new ArrayList<Pair<Float, Integer>>());
        }

        FileSystem fs = FileSystem.get(new Configuration());

        getTop(inputPath, fs);

        writeResult(fs, outputPath);

        //initialize(inputPath, fs);

        return 0;


    }
}
