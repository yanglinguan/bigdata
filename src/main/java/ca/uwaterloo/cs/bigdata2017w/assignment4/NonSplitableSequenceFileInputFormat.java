package ca.uwaterloo.cs.bigdata2017w.assignment4;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

/**
 * Created by yanglinguan on 17/2/2.
 */
public class NonSplitableSequenceFileInputFormat<K, V> extends SequenceFileInputFormat<K, V> {
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
