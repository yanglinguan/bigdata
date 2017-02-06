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
import org.apache.log4j.Logger;

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

    private static HashMap<Integer, ArrayList<Pair<Float, Integer>>> topList = new HashMap<>();

    private void addTopList(int s, float p, int id, int topValue) {
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

    private void getTop(String iterPath, FileSystem fs, int topValue) throws IOException {
        for (FileStatus f : fs.listStatus(new Path(iterPath))) {
            if (f.getPath().getName().startsWith("part-")) {
                LOG.info("file: " + f.getPath().getName());
                SequenceFile.Reader reader = new SequenceFile.Reader(fs, f.getPath(), fs.getConf());
                //FSDataInputStream fin = fs.open(f.getPath());

                while (true) {
                    try {
                        PersonalizedPageRankNode node = new PersonalizedPageRankNode();
                        IntWritable key = new IntWritable();

                        reader.next(key, node);

                        if(node.getType() != PageRankNode.Type.Complete) {
                            break;
                        }

                        for(int i = 0; i < SOURCE_NODES.size(); i++) {
                            if(node.getType() == PageRankNode.Type.Complete) {
                                addTopList(SOURCE_NODES.get(i), (float) StrictMath.exp(node.getPageRank(i)), node.getNodeId(), topValue);
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

    private void writeResult(FileSystem fs, String outPath, int topValue) {
        for(int s: SOURCE_NODES) {
            System.out.println("Source: " + s);
            for(int i = 0; i < topValue; i++) {
                String printString = String.format("%.5f %d", topList.get(s).get(i).getFirst(), topList.get(s).get(i).getSecond());
                System.out.println(printString);
            }
            System.out.println();
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

        for(String s : sourceNodes) {
            SOURCE_NODES.add(Integer.parseInt(s));
            topList.put(Integer.parseInt(s), new ArrayList<Pair<Float, Integer>>());
        }

        FileSystem fs = FileSystem.get(new Configuration());

        getTop(inputPath, fs, top);

        writeResult(fs, outputPath, top);

        return 0;
    }
}
