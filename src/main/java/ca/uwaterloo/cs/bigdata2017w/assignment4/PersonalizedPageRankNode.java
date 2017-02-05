package ca.uwaterloo.cs.bigdata2017w.assignment4;

import org.apache.hadoop.io.FloatWritable;
import sun.reflect.generics.reflectiveObjects.LazyReflectiveObjectGenerator;
import tl.lin.data.array.ArrayListOfFloats;
import tl.lin.data.array.ArrayListOfFloatsWritable;
import tl.lin.data.array.ArrayListOfIntsWritable;
import tl.lin.data.map.HMapIFW;
import tl.lin.data.map.HMapIVW;
import tl.lin.data.map.MapIF;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by yanglinguan on 17/2/2.
 */
public class PersonalizedPageRankNode extends PageRankNode {

   // private ArrayListOfFloatsWritable pagerank;

   // private ArrayListOfIntsWritable source;

    // private HMapIFW pageRank = new HMapIFW();

    private ArrayListOfFloatsWritable pageRank;


    public PersonalizedPageRankNode() {
        super();

    }

    public void setSourceNode(ArrayList<Integer> source) {
        pageRank = new ArrayListOfFloatsWritable();
        for(int s: source) {
            pageRank.add(Float.NEGATIVE_INFINITY);
        }
    }

    public float getPageRank(int source) {
        return pageRank.get(source);
    }

    public void setPageRank(int source, float p) {
        //pageRank.put(source, p);
        pageRank.set(source, p);

    }


    public void setPageRankList(ArrayListOfFloatsWritable prlist) {
        this.pageRank = prlist;
//        pageRank.clear();
//        for(float f: prlist) {
//            pageRank.add(f);
//        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int b = in.readByte();
        type =  mapping[b];
        nodeid = in.readInt();
       // int size = in.readInt();


        if (type.equals(Type.Mass)) {
            //pageRank = new HMapIFW();
            pageRank = new ArrayListOfFloatsWritable();
            pageRank.readFields(in);
//            source = new ArrayListOfIntsWritable();
//            source.readFields(in);
            //pagerank = in.readFloat();
            return;
        }

        if (type.equals(Type.Complete)) {
//            pageRank = new HMapIFW();
            pageRank = new ArrayListOfFloatsWritable();
            pageRank.readFields(in);
//            source = new ArrayListOfIntsWritable();
//            source.readFields(in);
        }



        adjacenyList = new ArrayListOfIntsWritable();
        adjacenyList.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(type.val);
        out.writeInt(nodeid);
        //out.writeInt(pagerank.size());

        if (type.equals(Type.Mass)) {
            pageRank.write(out);
          //  source.write(out);
            //out.writeFloat(pagerank);
            return;
        }

        if (type.equals(Type.Complete)) {
            //out.writeFloat(pagerank);
            pageRank.write(out);
//            source.write(out);
        }



        adjacenyList.write(out);
    }


//    public void setSourcePageRank(int sourceID, float pageRank) {
//        sourceNodePageRank.put(sourceID, pageRank);
//    }
}
