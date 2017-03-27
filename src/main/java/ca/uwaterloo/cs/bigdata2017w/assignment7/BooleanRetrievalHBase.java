/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.uwaterloo.cs.bigdata2017w.assignment7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfWritables;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

public class BooleanRetrievalHBase extends Configured implements Tool {

  private Stack<Set<Integer>> stack;
  private Table index;
  private Table collection;

  private BooleanRetrievalHBase() {}

  private void initialize(String indexPath, String collectionPath, Connection connection) throws IOException {
      index = connection.getTable(TableName.valueOf(indexPath));
      collection = connection.getTable(TableName.valueOf(collectionPath));
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
      PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>> value = new PairOfWritables<>();

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

  private static final class Args {
    @Option(name = "-config", metaVar = "[path]", required = true, usage = "HBase config")
    String config;

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

    Configuration conf = getConf();
    conf.addResource(new Path(args.config));

    Configuration hbaseConfig = HBaseConfiguration.create(conf);
    Connection connection = ConnectionFactory.createConnection(hbaseConfig);

    initialize(args.index, args.collection, connection);

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
    ToolRunner.run(new BooleanRetrievalHBase(), args);
  }
}
