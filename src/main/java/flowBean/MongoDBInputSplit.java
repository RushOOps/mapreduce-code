package flowBean;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MongoDBInputSplit extends InputSplit implements Writable {

    private long start;
    private long end;

    public long getStart() {
        return start;
    }
    public long getEnd(){
        return end;
    }

    public MongoDBInputSplit(){}

    public MongoDBInputSplit(long start, long end){
        this.start = start;
        this.end = end;
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
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.start = dataInput.readLong();
        this.end = dataInput.readLong();
    }
}
