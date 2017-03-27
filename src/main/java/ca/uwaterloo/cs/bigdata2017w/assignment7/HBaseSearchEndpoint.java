package ca.uwaterloo.cs.bigdata2017w.assignment7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.eclipse.jetty.server.Server;
//import org.eclipse.jetty.servlet.ServletContextHandler;
//import org.eclipse.jetty.servlet.ServletHolder;
//import org.glassfish.jersey.server.ResourceConfig;
//import org.glassfish.jersey.servlet.ServletContainer;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
//import org.springframework.boot.SpringApplication;
//import org.springframework.boot.autoconfigure.SpringBootApplication;
//import org.springframework.web.bind.annotation.RequestMapping;
//import org.springframework.web.bind.annotation.RestController;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfInts;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.*;

/**
 * Created by yanglinguan on 2017-03-26.
 */


//@SpringBootApplication
//@RestController
//public class HBaseSearchEndpoint extends Configured implements Tool {
//
////    private Stack<Set<Integer>> stack;
////    private Table index;
////    private Table collection;
//
//    private HBaseSearchEndpoint() {}
//
//    private static final class Args {
//        @Option(name = "-config", metaVar = "[path]", required = true, usage = "HBase config")
//        public String config;
//
//        @Option(name = "-index", metaVar = "[path]", required = true, usage = "index path")
//        String index;
//
//        @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
//        String collection;
//
//        @Option(name = "-port", metaVar = "[]", required = true, usage = "port")
//        int port;
//    }
//
////    @RequestMapping("/")
////    public String home() {
////        return "Hello World!";
////    }
//
//    @Override
//    public int run(String[] argv) throws Exception {
//
//        final Args args = new Args();
//        CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));
//
//        try {
//            parser.parseArgument(argv);
//        } catch (CmdLineException e) {
//            System.err.println(e.getMessage());
//            parser.printUsage(System.err);
//            return -1;
//        }
//
//        if (args.collection.endsWith(".gz")) {
//            System.out.println("gzipped collection is not seekable: use compressed version!");
//            return -1;
//        }
//
//        Configuration conf = getConf();
//        conf.addResource(new Path(args.config));
//
//        Configuration hbaseConfig = HBaseConfiguration.create(conf);
//        Connection connection = ConnectionFactory.createConnection(hbaseConfig);
////        index = connection.getTable(TableName.valueOf(args.index));
////        collection = connection.getTable(TableName.valueOf(args.collection));
////        stack = new Stack<>();
//
////        SpringApplication.run(HBaseSearchEndpoint.class, argv);
//
////        ResourceConfig config = new ResourceConfig();
////        config.packages("ca.uwaterloo.cs.bigdata2017w.assignment7");
////
////        Map<String,Object> initMap = new HashMap<>();
////        initMap.put("index", connection.getTable(TableName.valueOf(args.index)));
////        initMap.put("connection", connection.getTable(TableName.valueOf(args.collection)));
////
////        config.setProperties(initMap);
////
////        ServletHolder servlet = new ServletHolder(new ServletContainer(config));
////        Server server = new Server(args.port);
////        ServletContextHandler context = new ServletContextHandler(server, "/*");
////        context.addServlet(servlet, "/*");
////
////        try {
////            server.start();
////            server.join();
////        } finally {
////            server.destroy();
////        }
//
//
//        return 1;
//    }
//
//
//    public static void main(String[] args) throws Exception {
//        //new JHades().overlappingJarsReport();
//        ToolRunner.run(new HBaseSearchEndpoint(), args);
//    }
//}


public class HBaseSearchEndpoint extends Configured implements Tool {

    private Stack<Set<Integer>> stack;
    private Table index;
    private Table collection;

    private HBaseSearchEndpoint() {}

    private HashMap<Integer, String> runQuery(String q) throws IOException {
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
        HashMap<Integer, String> result = new HashMap<>();

        for (Integer i : set) {
            String line = fetchLine(i);
            result.put(i, line);
            System.out.println(i + "\t" + line);
        }
        return result;
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

        ArrayListWritable<PairOfInts> x = new ArrayListWritable<>();

        Get get = new Get(Bytes.toBytes(term));
        Result result = index.get(get);
        byte[] posting = result.getValue(BuildInvertedIndexHBase.PO, BuildInvertedIndexHBase.POSTING);

        DataInputStream input = new DataInputStream(new ByteArrayInputStream(posting));

        int predocSum = 0;
        while(true) {
            try{
                int docno = WritableUtils.readVInt(input);
                int tf = WritableUtils.readVInt(input);
                if(tf != 0 ) {
                    predocSum += docno;
                    x.add(new PairOfInts(predocSum, tf));
                }
            } catch( EOFException e){
                break;
            }
        }

        return x;
    }

    private String fetchLine(long offset) throws IOException {
        Get get = new Get(Bytes.toBytes(offset));
        Result result = collection.get(get);
        String d = Bytes.toString(result.getValue(InsertCollectionHBase.DOC, InsertCollectionHBase.DOCUMENT));
        return d.length() > 80 ? d.substring(0, 80) + "..." : d;
    }

    private static final class Args {
        @Option(name = "-config", metaVar = "[path]", required = true, usage = "HBase config")
        public String config;

        @Option(name = "-index", metaVar = "[path]", required = true, usage = "index path")
        String index;

        @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
        String collection;

        @Option(name = "-port", metaVar = "[]", required = true, usage = "port")
        int port;
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

        Configuration conf = getConf();
        conf.addResource(new Path(args.config));

        Configuration hbaseConfig = HBaseConfiguration.create(conf);
        Connection connection = ConnectionFactory.createConnection(hbaseConfig);
        index = connection.getTable(TableName.valueOf(args.index));
        collection = connection.getTable(TableName.valueOf(args.collection));
        stack = new Stack<>();



        return 1;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new HBaseSearchEndpoint(), args);
    }
}
