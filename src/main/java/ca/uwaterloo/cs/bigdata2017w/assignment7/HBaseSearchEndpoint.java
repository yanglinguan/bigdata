package ca.uwaterloo.cs.bigdata2017w.assignment7;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.ServletHolder;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfInts;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.*;

/**
 * Created by yanglinguan on 2017-03-26.
 */


public class HBaseSearchEndpoint
        extends Configured
        implements Tool
{

    private Stack<Set<Integer>> stack;
    private Table index;
    private Table collection;

    public class QueryResult {
        public int docid;
        public String text;
        public QueryResult(int docid, String text) {
            this.docid = docid;
            this.text = text;
        }

    }


    private HBaseSearchEndpoint() {}

    private void initialize(String indexPath, String collectionPath, Connection connection) throws IOException {
        index = connection.getTable(TableName.valueOf(indexPath));
        collection = connection.getTable(TableName.valueOf(collectionPath));
        stack = new Stack<>();
    }

    private List<QueryResult> runQuery(String q) throws IOException {
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

        List<QueryResult> results = new ArrayList<>();

        for (Integer i : set) {
            String line = fetchLine(i);
            QueryResult r = new QueryResult(i, line);
            results.add(r);
//            System.out.println(i + "\t" + line);
        }
        return results;
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
        //PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>> value = new PairOfWritables<>();

        ArrayListWritable<PairOfInts> x = new ArrayListWritable<>();
        key.set(term);
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




    public class HomeServlet extends HttpServlet {

        public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException,
                IOException {

            String q = req.getParameterValues("query")[0];
//            if (req.getParameterValues("query") != null) {
//                System.out.println(req.getParameterValues("query")[0]);
//            }
            List<QueryResult> results = runQuery(q);
//            List<QueryResult> results = new ArrayList<>();
//            results.add(new QueryResult(425450, "  CELIA. No; when Nature hath made a fair creature, may she not by"));
//            results.add(new QueryResult(1553567, "    Disguise fair nature with hard-favour'd rage;"));
//            results.add(new QueryResult(5327159, "  Showing fair nature is both kind and tame;"));

            res.setCharacterEncoding("UTF-8");
            res.setContentType("application/json");
            PrintWriter out = res.getWriter();

            GsonBuilder builder = new GsonBuilder();
            builder.disableHtmlEscaping();
            Gson gson = builder.create();
            String json = gson.toJson(results);

            out.println(json);
            out.close();
        }

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
            System.out.println("herer");
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

        initialize(args.index, args.collection, connection);

        Server server = new Server(args.port);

        org.mortbay.jetty.servlet.Context root = new org.mortbay.jetty.servlet.Context(server, "/",
                org.mortbay.jetty.servlet.Context.SESSIONS);

        root.addServlet(new ServletHolder(new HomeServlet()), "/search");


        try {
            server.start();
            server.join();
        } finally {
            server.destroy();
        }

        return 1;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] arg) throws Exception {
        ToolRunner.run(new HBaseSearchEndpoint(), arg);
    }
}
