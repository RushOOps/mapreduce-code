package mapreduce.groupSort;

import flowBean.IntPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class GroupSortPartitioner extends Partitioner<IntPair, IntWritable> {
    @Override
    public int getPartition(IntPair intPair, IntWritable intWritable, int numPartitions) {
        return (intPair.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
