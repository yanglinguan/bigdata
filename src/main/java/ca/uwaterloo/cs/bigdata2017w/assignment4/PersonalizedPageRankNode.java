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

    private HMapIFW pageRank = new HMapIFW();


    public PersonalizedPageRankNode() {
        super();

       // pagerank = new ArrayListOfFloatsWritable(sourceNum);
    }

    public void setSourceNode(ArrayList<Integer> source) {
        for(int s : source) {
            pageRank.put(s, Float.NEGATIVE_INFINITY);
        }
    }

//    public void setSourceNode(int sourceNum) {
//        pagerank = new ArrayListOfFloatsWritable(sourceNum);
//        source  = new ArrayListOfIntsWritable(sourceNum);
//       // pageRank = new HMapIVW<>();
//    }

//    public void setSource(ArrayListOfIntsWritable s) {
//        this.source = s;
//    }

//    public ArrayListOfIntsWritable getSource() {
//        return this.source;
//    }

    public float getPageRank(int source) {
        return pageRank.get(source);
    }

    public void setPageRank(int source, float p) {
        pageRank.put(source, p);
    }

    public void setPageRankList(HMapIFW prlist) {
        if(prlist.size() != 3) {
            System.out.println("pagerank size != 3**********************************");
        }
        for(MapIF.Entry e: prlist.entrySet()) {
            this.pageRank.put(e.getKey(), e.getValue());
        }
        //this.pageRank = prlist;
    }



//    public void setPageRank(int idx, float p) {
//        this.pagerank.set(idx, p);
//       // this.pageRank.put(idx, p);
//    }

//    public void setPageRankList(ArrayListOfFloatsWritable pageRankList){
//        this.pagerank = pageRankList;
//    }

//    public int getSourceIndex(int s) {
//        return source.indexOf(s);
//    }

//    public float getPageRankForSource(int s) {
//        return pagerank.get(source.indexOf(s));
//    }

//    public ArrayListOfFloatsWritable getPageRankList() {
//        return this.pagerank;
//    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int b = in.readByte();
        type =  mapping[b];
        nodeid = in.readInt();
       // int size = in.readInt();


        if (type.equals(Type.Mass)) {
            pageRank = new HMapIFW();
            pageRank.readFields(in);
//            source = new ArrayListOfIntsWritable();
//            source.readFields(in);
            //pagerank = in.readFloat();
            return;
        }

        if (type.equals(Type.Complete)) {
            pageRank = new HMapIFW();
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
