package ca.uwaterloo.cs.bigdata2017w.assignment4;

import tl.lin.data.array.ArrayListOfFloatsWritable;
import tl.lin.data.array.ArrayListOfIntsWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by yanglinguan on 17/2/2.
 */
public class PersonalizedPageRankNode extends PageRankNode {

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
        pageRank.set(source, p);

    }


    public void setPageRankList(ArrayListOfFloatsWritable prlist) {
        this.pageRank = prlist;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int b = in.readByte();
        type =  mapping[b];
        nodeid = in.readInt();


        if (type.equals(Type.Mass)) {
            pageRank = new ArrayListOfFloatsWritable();
            pageRank.readFields(in);
            return;
        }

        if (type.equals(Type.Complete)) {
            pageRank = new ArrayListOfFloatsWritable();
            pageRank.readFields(in);
        }



        adjacenyList = new ArrayListOfIntsWritable();
        adjacenyList.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(type.val);
        out.writeInt(nodeid);

        if (type.equals(Type.Mass)) {
            pageRank.write(out);
            return;
        }

        if (type.equals(Type.Complete)) {
            pageRank.write(out);
        }



        adjacenyList.write(out);
    }
}
