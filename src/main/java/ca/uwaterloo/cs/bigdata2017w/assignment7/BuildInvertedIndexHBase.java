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

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfStringInt;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class BuildInvertedIndexHBase extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(BuildInvertedIndexHBase.class);

    public static final String[] FAMILIES = { "doc", "posting" };
    public static final byte[] DF = FAMILIES[0].getBytes();
    public static final byte[] PO = FAMILIES[1].getBytes();
    public static final byte[] FREQUENCY = "frequency".getBytes();
    public static final byte[] POSTING = "posting".getBytes();


    private static final class MyMapper extends Mapper<LongWritable, Text, PairOfStringInt, IntWritable> {
        private static final IntWritable OUTVALUE = new IntWritable();
        private static final PairOfStringInt OUTKEY = new PairOfStringInt();
        private static final Object2IntFrequencyDistribution<String> COUNTS =
                new Object2IntFrequencyDistributionEntry<>();

        @Override
        public void map(LongWritable docno, Text doc, Context context)
                throws IOException, InterruptedException {
            List<String> tokens = Tokenizer.tokenize(doc.toString());

            // Build a histogram of the terms.
            COUNTS.clear();
            for (String token : tokens) {
                COUNTS.increment(token);
            }

            // Emit postings.
            for (PairOfObjectInt<String> e : COUNTS) {
                OUTKEY.set(e.getLeftElement(), (int) docno.get());
                OUTVALUE.set(e.getRightElement());
                context.write(OUTKEY, OUTVALUE);
            }
        }
    }

    public static class MyTableReducer extends TableReducer<PairOfStringInt, IntWritable, ImmutableBytesWritable>  {
        private static String preTerm = "";
        private static int preDocNo = 0;
        private static ByteArrayOutputStream b = new ByteArrayOutputStream();
        private static DataOutputStream postings = new DataOutputStream(b);
        private static int df = 0;

        @Override
        public void setup(Context context) throws IOException {
            preTerm = "";
            preDocNo = 0;
            df = 0;
            b = new ByteArrayOutputStream();
            postings = new DataOutputStream(b);
        }

        @Override
        public void reduce(PairOfStringInt key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            Iterator<IntWritable> iter = values.iterator();

            int tf = 0;
            while (iter.hasNext()) {
                tf += iter.next().get();
            }

            if(!key.getLeftElement().equals(preTerm) && !preTerm.equals("")) {

                Put put = new Put(Bytes.toBytes(preTerm));
                put.addColumn(DF, FREQUENCY, Bytes.toBytes(df));
                put.addColumn(PO, POSTING, b.toByteArray());
                context.write(null, put);

                postings.flush();
                b.reset();
                df = 0;
                preDocNo = 0;
            }

            WritableUtils.writeVInt(postings, key.getRightElement() - preDocNo);
            WritableUtils.writeVInt(postings, tf);
            df++;

            preTerm = key.getLeftElement();
            preDocNo = key.getRightElement();


        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            Put put = new Put(Bytes.toBytes(preTerm));
            put.addColumn(DF, FREQUENCY, Bytes.toBytes(df));
            put.addColumn(PO, POSTING, b.toByteArray());
            context.write(null, put);
        }

    }


    private static final class MyPartitioner extends Partitioner<PairOfStringInt, IntWritable> {
        @Override
        public int getPartition(PairOfStringInt key, IntWritable value, int numReduceTasks) {
            return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    private BuildInvertedIndexHBase() {}

    public static class Args {
        @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
        public String input;

        @Option(name = "-index", metaVar = "[name]", required = true, usage = "HBase table to store output")
        public String index;

        @Option(name = "-config", metaVar = "[path]", required = true, usage = "HBase config")
        public String config;

        @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
        public int numReducers = 1;
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

        LOG.info("Tool: " + BuildInvertedIndexHBase.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output table: " + args.index);
        LOG.info(" - config: " + args.config);
        LOG.info(" - number of reducers: " + args.numReducers);

        // If the table doesn't already exist, create it.
        Configuration conf = getConf();
        conf.addResource(new Path(args.config));

        Configuration hbaseConfig = HBaseConfiguration.create(conf);
        Connection connection = ConnectionFactory.createConnection(hbaseConfig);
        Admin admin = connection.getAdmin();

        if (admin.tableExists(TableName.valueOf(args.index))) {
            LOG.info(String.format("Table '%s' exists: dropping table and recreating.", args.index));
            LOG.info(String.format("Disabling table '%s'", args.index));
            admin.disableTable(TableName.valueOf(args.index));
            LOG.info(String.format("Droppping table '%s'", args.index));
            admin.deleteTable(TableName.valueOf(args.index));
        }

        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(args.index));
        for (int i = 0; i < FAMILIES.length; i++) {
            HColumnDescriptor hColumnDesc = new HColumnDescriptor(FAMILIES[i]);
            tableDesc.addFamily(hColumnDesc);
        }
        admin.createTable(tableDesc);
        LOG.info(String.format("Successfully created table '%s'", args.index));

        admin.close();

        // Now we're ready to start running MapReduce.
        Job job = Job.getInstance(getConf());
        job.setJobName(BuildInvertedIndexHBase.class.getSimpleName());
        job.setJarByClass(BuildInvertedIndexHBase.class);



        job.setMapOutputKeyClass(PairOfStringInt.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(MyMapper.class);
        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(job, new Path(args.input));
        TableMapReduceUtil.initTableReducerJob(args.index, MyTableReducer.class, job);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

    /**
    * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
    */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new BuildInvertedIndexHBase(), args);
    }
}
