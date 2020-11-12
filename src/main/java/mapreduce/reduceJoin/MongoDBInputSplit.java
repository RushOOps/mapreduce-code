package mapreduce.reduceJoin;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MongoDBInputSplit extends InputSplit implements Writable {

    private long start;
    private long end;
    private String collection;

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public MongoDBInputSplit(){}

    public MongoDBInputSplit(long start, long end, String collection){
        this.start = start;
        this.end = end;
        this.collection = collection;
    }

    @Override
    public long getLength() {
        return this.end - this.start;
    }

    @Override
    public String[] getLocations() {
        return new String[0];
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(start);
        dataOutput.writeLong(end);
        dataOutput.writeUTF(collection);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.start = dataInput.readLong();
        this.end = dataInput.readLong();
        this.collection = dataInput.readUTF();
    }
}
