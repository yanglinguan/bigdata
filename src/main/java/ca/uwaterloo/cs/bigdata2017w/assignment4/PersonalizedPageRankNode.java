package ca.uwaterloo.cs.bigdata2017w.assignment4;

import org.apache.hadoop.io.FloatWritable;
import tl.lin.data.array.ArrayListOfFloats;
import tl.lin.data.array.ArrayListOfFloatsWritable;
import tl.lin.data.array.ArrayListOfIntsWritable;
import tl.lin.data.map.HMapIVW;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by yanglinguan on 17/2/2.
 */
public class PersonalizedPageRankNode extends PageRankNode{

    private ArrayListOfFloatsWritable pagerank;

    private ArrayListOfIntsWritable source;

   // private HMapIVW<FloatWritable> pageRank;


    public PersonalizedPageRankNode() {
        super();
       // pagerank = new ArrayListOfFloatsWritable(sourceNum);
    }

    public void setSourceNode(int sourceNum) {
        pagerank = new ArrayListOfFloatsWritable(sourceNum);
        source  = new ArrayListOfIntsWritable(sourceNum);
       // pageRank = new HMapIVW<>();
    }

    public void setSource(ArrayListOfIntsWritable s) {
        this.source = s;
    }

    public ArrayListOfIntsWritable getSource() {
        return this.source;
    }


    public void setPageRank(int idx, float p) {
        this.pagerank.set(idx, p);
       // this.pageRank.put(idx, p);
    }

    public void setPageRankList(ArrayListOfFloatsWritable pageRankList){
        this.pagerank = pageRankList;
    }

    public int getSourceIndex(int s) {
        return source.indexOf(s);
    }

    public float getPageRankForSource(int s) {
        return pagerank.get(source.indexOf(s));
    }

    public ArrayListOfFloatsWritable getPageRankList() {
        return this.pagerank;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int b = in.readByte();
        type =  mapping[b];
        nodeid = in.readInt();
        int size = in.readInt();


        pagerank = new ArrayListOfFloatsWritable(size);

        if (type.equals(Type.Mass)) {
            pagerank.readFields(in);
            source = new ArrayListOfIntsWritable();
            source.readFields(in);
            //pagerank = in.readFloat();
            return;
        }

        if (type.equals(Type.Complete)) {
            pagerank.readFields(in);
            source = new ArrayListOfIntsWritable();
            source.readFields(in);
        }



        adjacenyList = new ArrayListOfIntsWritable();
        adjacenyList.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(type.val);
        out.writeInt(nodeid);
        out.writeInt(pagerank.size());

        if (type.equals(Type.Mass)) {
            pagerank.write(out);
            source.write(out);
            //out.writeFloat(pagerank);
            return;
        }

        if (type.equals(Type.Complete)) {
            //out.writeFloat(pagerank);
            pagerank.write(out);
            source.write(out);
        }



        adjacenyList.write(out);
    }


//    public void setSourcePageRank(int sourceID, float pageRank) {
//        sourceNodePageRank.put(sourceID, pageRank);
//    }
}
