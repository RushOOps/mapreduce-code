package mapreduce.topN;

import org.apache.hadoop.mapreduce.Partitioner;


public class TopNPartitioner extends Partitioner<TopNBean, Integer>{
    @Override
    public int getPartition(TopNBean topNBean, Integer integer, int numPartitions) {
        return (topNBean.getCourse().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
