package ca.uwaterloo.cs.bigdata2017w.assignment3;

/**
 * Created by yanglinguan on 17/1/31.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import scala.util.hashing.ByteswapHashing;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfWritables;

import java.awt.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

public class BooleanRetrievalCompressed extends Configured implements Tool {
    private ArrayList<MapFile.Reader> index;
    private FSDataInputStream collection;
    private Stack<Set<Integer>> stack;

    private BooleanRetrievalCompressed() {}

    private void initialize(String indexPath, String collectionPath, FileSystem fs) throws IOException {
        File f = new File(indexPath);
        index = new ArrayList<>();
        //System.out.println("number of files " + files);

        int files = f.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().startsWith("part-r-");
            }
        }).length;

//        for(File x: files) {
//            index.add(new MapFile.Reader(new Path(indexPath + "/" + x.getName()), fs.getConf()));
//        }

        System.out.println("number of files " + files);
        for(int i = 0; i < files; i++) {
            index.add(new MapFile.Reader(new Path(indexPath + "/part-r-0000" + i), fs.getConf()));
        }
       // index = new MapFile.Reader(new Path(indexPath + "/part-r-00000"), fs.getConf());
        collection = fs.open(new Path(collectionPath));
        stack = new Stack<>();
    }

    private void runQuery(String q) throws IOException {
        String[] terms = q.split("\\s+");

        for (String t : terms) {
            if (t.equals("AND")) {
                performAND();
            } else if (t.equals("OR")) {
                performOR();
            } else {
                pushTerm(t);
            }
        }

        Set<Integer> set = stack.pop();

        for (Integer i : set) {
            String line = fetchLine(i);
            System.out.println(i + "\t" + line);
        }
    }

    private void pushTerm(String term) throws IOException {
        stack.push(fetchDocumentSet(term));
    }

    private void performAND() {
        Set<Integer> s1 = stack.pop();
        Set<Integer> s2 = stack.pop();

        Set<Integer> sn = new TreeSet<>();

        for (int n : s1) {
            if (s2.contains(n)) {
                sn.add(n);
            }
        }

        stack.push(sn);
    }

    private void performOR() {
        Set<Integer> s1 = stack.pop();
        Set<Integer> s2 = stack.pop();

        Set<Integer> sn = new TreeSet<>();

        for (int n : s1) {
            sn.add(n);
        }

        for (int n : s2) {
            sn.add(n);
        }

        stack.push(sn);
    }

    private Set<Integer> fetchDocumentSet(String term) throws IOException {
        Set<Integer> set = new TreeSet<>();

        for (PairOfInts pair : fetchPostings(term)) {
            set.add(pair.getLeftElement());
        }

        return set;
    }

    private ArrayListWritable<PairOfInts> fetchPostings(String term) throws IOException {
        Text key = new Text();
        PairOfWritables<IntWritable, BytesWritable> value =
                new PairOfWritables<>();

        ArrayListWritable<PairOfInts> x = new ArrayListWritable<>();

        key.set(term);
      //  System.out.println("key: " + term);
        WritableUtils wu = new WritableUtils();

        for(MapFile.Reader e: index) {
            Writable t = e.get(key, value);
        //    System.out.println("hello");
            if(t != null) {
                break;
            }
        }


     //   System.out.println("df: " + value.getLeftElement().get());

        DataInputStream input = new DataInputStream(new ByteArrayInputStream(value.getRightElement().getBytes()));

       // System.out.println(input.read());
      //  System.out.println("size: " + value.getRightElement().getBytes().length);
        int predocSum = 0;
        while(true) {
            try{
                int docno = wu.readVInt(input);
                int tf = wu.readVInt(input);
                if(tf != 0 ) {
                    predocSum += docno;
                    x.add(new PairOfInts(predocSum, tf));
                }
               // System.out.println("predocSum: " + predocSum + ", tf: " + tf);

            } catch( EOFException e){
                break;
            }
        }
       // System.out.println("1: " + wu.readVInt(input));
        //System.out.println("2: " + wu.readVInt(input));

       // System.out.println(wu.readVIntInRange(input, 0, input.read()));

        //value.getRightElement();

       // ArrayListWritable<PairOfInts> x = new ArrayListWritable<>();

       // index.get(key, value);

        return x;
    }

    public String fetchLine(long offset) throws IOException {
        collection.seek(offset);
        BufferedReader reader = new BufferedReader(new InputStreamReader(collection));

        String d = reader.readLine();
        return d.length() > 80 ? d.substring(0, 80) + "..." : d;
    }

    private static final class Args {
        @Option(name = "-index", metaVar = "[path]", required = true, usage = "index path")
        String index;

        @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
        String collection;

        @Option(name = "-query", metaVar = "[term]", required = true, usage = "query")
        String query;
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

        if (args.collection.endsWith(".gz")) {
            System.out.println("gzipped collection is not seekable: use compressed version!");
            return -1;
        }

        FileSystem fs = FileSystem.get(new Configuration());

        initialize(args.index, args.collection, fs);

        System.out.println("Query: " + args.query);
        long startTime = System.currentTimeMillis();
        runQuery(args.query);
        System.out.println("\nquery completed in " + (System.currentTimeMillis() - startTime) + "ms");

        return 1;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new BooleanRetrievalCompressed(), args);
    }
}

